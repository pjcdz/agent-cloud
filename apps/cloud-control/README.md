# Cloud control

`ControlStore.mutate` is the shared durable lifecycle entry point for create,
start and stop. It commits the caller's mutation key, response, logical server,
desired state, active run and event in one PostgreSQL transaction. Repeating
the same key and body returns the original response. Ownership is checked for
existing servers; a changed operation or body raises a 409 `ControlError`.

## REST create and status

The machine-token bearer identity can create a logical server asynchronously:

```sh
curl -X POST "$CONTROL_URL/v1/servers" \
  -H "Authorization: Bearer $CLOUD_MACHINE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"client_request_id":"create-one","name":"one","eula_accepted":true}'
```

Creation returns `202` with `request_id`, `server_id`, a typed state and
`status_url`. Explicit EULA acceptance is required and included in the durable
mutation evidence. Omitting it returns `409` with `action_required` set to
`accept_eula`.

Poll the returned status URL with the same bearer token:

```sh
curl -H "Authorization: Bearer $CLOUD_MACHINE_TOKEN" \
  "$CONTROL_URL/v1/servers/<server_id>"
```

Status is owner-scoped. It reports the current `cold` allocation path and only
includes `endpoint` after the reconciler has observed Minecraft protocol
readiness and changed the state to `running`. Warm allocation remains TES-153.

## REST start and stop

Start and stop record the owner's intent and return `202` before any runtime
call:

```sh
curl -X POST "$CONTROL_URL/v1/servers/<server_id>/stop" \
  -H "Authorization: Bearer $CLOUD_MACHINE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"client_request_id":"stop-one"}'
```

`/start` takes the same body. Both use the durable `client_request_id` rules
above, so a retried request returns the original response. Stop reports
`stopping` until the reconciler observes an exited workload; start reuses the
server's world and endpoint.

## MCP tools

`POST /mcp` serves MCP Streamable HTTP in stateless JSON mode with the same
bearer machine token. It exposes `minecraft_create`, `minecraft_start`,
`minecraft_stop` and `minecraft_status`. Their arguments match the REST bodies
plus `server_id`, and they call the same durable mutation and status code.
Results carry the REST response as `structuredContent`; control errors return
`isError: true` with the same `error` codes. Point an MCP client at
`$CONTROL_URL/mcp` with an `Authorization: Bearer` header.

## CLI

`src/cli.ts` is a REST client for operators. It runs on the host, reads
`CLOUD_MACHINE_TOKEN` and `CLOUD_CONTROL_URL` (default
`http://127.0.0.1:3000`) from the environment, and never receives Docker or
host credentials. From the repository root, after `npm run build`:

```sh
npm run -s cloud -- create one --accept-eula --wait
npm run -s cloud -- status <server_id>
npm run -s cloud -- stop <server_id> --wait
npm run -s cloud -- start <server_id> --wait
```

Each command prints JSON lines. Mutations print the accepted response, which
includes `request_id`. Pass `--request-id` to retry with the same durable
`client_request_id`; without it a new UUID is used. `--wait` polls status and
prints it once the server is `running`, with its endpoint, or `stopped`. It
has no deadline; interrupt it to stop polling. Control errors go to stderr
with exit code 1; usage and missing-token errors exit with code 2.

`Reconciler.reconcile` observes the runtime before applying one missing effect.
The runtime receives the stable logical server and world identities. Its
create, start and stop operations must be idempotent under those identities.
The reconciler waits while startup or termination is already in progress.
It marks a run stopped only after observing an absent or stopped workload.

A PostgreSQL session advisory lock serializes reconciliation for one server.
No database transaction spans a runtime call. Observation writes check the
desired-state generation so an older observation cannot replace newer intent.
`tick` visits managed servers, including converged ones so runtime drift can
be observed, and reports failures after allowing other servers to progress.

With `CLOUD_RUNTIME=kubernetes`, the process reconciles every second through
its projected service-account token and cluster CA. It loads the pinned Paper
recipe from `catalog/games/minecraft-java/paper/k8s/deployment.yaml`; the image
includes that file. `CLOUD_PAPER_RECIPE` can override its path for development.
The installer applies the retained local-path StorageClass. Each logical
server has its own deterministic Deployment, PVC and Service. Create retries
preserve existing resources and replicas. Stop retains both the PVC and Service;
start waits until all old Pods are gone. The private host port also reserves
the single qualified worker slot during termination.

Readiness comes from the recipe's Minecraft protocol probe. TES-69 returns an
internal Service address; TES-77 supplies the stable external endpoint. REST,
MCP and the CLI use the shared contract. PostgreSQL tests use a controllable fake
runtime; live Kubernetes acceptance has a separate operator probe.

From the repository root:

```sh
npm ci
npm run check
TEST_DATABASE_URL='<disposable PostgreSQL connection URL>' npm run test:integration
```

The integration tests create and remove isolated schemas. They cover delayed
termination, world identity across restart, an effect completed before a
control restart, intent changed during a start, stale readiness and progress
past an individual runtime failure. TES-70 also sends twenty concurrent copies
of each create/stop/start mutation through two database pools and races ten
reconciliations at each stage. It verifies one effect per operation, one active
run, durable response replay and a 409 control error for a changed body or
operation. HTTP transport and real K3s repetitions remain F3/F4 checks. The
TES-71 campaign starts a separate Node process running the production store
and reconciler, with a fake runtime served over IPC. It sends `SIGKILL` before
and after observation and each create/start/stop effect: twelve crash cases.
For an after-effect crash, the fake resource changes before the child receives
an acknowledgement. Fresh processes then recover against the same PostgreSQL
records and runtime resources.

Every case checks the final desired state, stable world identity, unchanged
response replay, a traceable request ID and at most one active run. The fake
runtime rejects a repeated effect. This proves process recovery for the
shared contract. [TES-69 live evidence](../../infra/evidence/tes-69-kubernetes-runtime.md)
covers Kubernetes lifecycle retries and control restarts. F4 retains the
external client acceptance campaign.

With `CLOUD_RUNTIME=docker`, the process uses the Docker Engine API over
`DOCKER_SOCKET_PATH` (default `/var/run/docker.sock`). It discovers the fixed
Compose service by `CLOUD_COMPOSE_PROJECT` and `CLOUD_COMPOSE_SERVICE`, verifies
the container name and persistent `/data` volume, and never acts on an
identity-mismatched container. The Docker healthcheck is the Minecraft
protocol readiness gate. `CLOUD_MAX_SERVERS=1` provides the single-installation
capacity boundary used by the Compose quickstart. See
[`infra/compose/README.md`](../../infra/compose/README.md) for the operator
commands and credential boundary.

## Managed beta account quota

Set `CLOUD_ACCOUNT_QUOTA=beta` on `cloud-control` to enforce the approved D3
policy: one logical server and one active server per authenticated principal.
Create and start serialize the account quota check in the same PostgreSQL
transaction as the intended state and request result. An excess returns
`409 quota_exceeded` through REST, MCP or the CLI. Replaying an accepted request
returns its stored result, and starting the same server again consumes no
additional account slot.

A stopped logical server still occupies its account's logical-server slot.
An execution continues occupying the active slot until its stop is observed;
requesting a stop alone does not free it. Pre-existing offline server records
are retained when enabling the policy, and their starts compete for the one
active slot. No records or worlds are deleted to satisfy a quota.

The setting is optional and does not alter the Compose quickstart's
`CLOUD_MAX_SERVERS=1` capacity result, `capacity_unavailable`. It does not add
installation capacity, managed accounts, a FIFO queue or beta deployment.
The PostgreSQL quota tests use two independent stores and verify competing
creates and starts, stop completion, account separation and request replay.
