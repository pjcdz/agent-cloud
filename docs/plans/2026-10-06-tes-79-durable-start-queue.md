# TES-79 durable start queue

[TES-79](https://linear.app/testingasdad/issue/TES-79/e312-add-start-requests-and-a-fifo-queue)
requires durable arrival, state, reservation and expiry metadata, with
ten equivalent requests retaining FIFO order through a control restart. Its
parent TES-33 requires the approved account rules and admission when capacity
is scarce. D3 approves five-minute turn confirmation. TES-82 consumes queue
state, valid requests ahead, expiry and cancellation through polling.

The implementation contract was agreed on 2026-10-06 from the current Linear
descriptions captured in `cloud-queue-79-82-2026-10-06.md` and
`cloud-shortlist-criteria-2026-10-06.md`. Repository decisions and the selected
profile are in `docs/decisions.md` and `benchmarks/minecraft/free-profile.json`.

## Contract

`CLOUD_START_QUEUE=beta` enables the backend. Foundation defaults and Compose
configuration stay unchanged. One installation has one qualified slot for
`paper-26.2-121` / `two-player`: 2 CPU quota units, a 2 GiB heap and a 3 GiB
container limit. This boundary does not measure physical capacity on a new
host, derive slots from available CPU/RAM, or apply the 4 GB storage threshold.
Existing creation storage admission remains owned by TES-86.

Requests have states `queued`, `reserved`, `admitted`, `cancelled`, `expired`
and `finished`. Database arrival time is recorded after acquiring the admission
lock. A durable integer order defines FIFO even when arrival times tie.
Cancellation and expiry retain history; a new attempt joins the tail.

An incoming request is admitted immediately only when there is no unfinished
run, admitted request, live reservation or earlier queued request. Existing
unfinished foundation runs also occupy capacity. An available turn goes to
the oldest queued request and reserves the slot for five minutes. Waiting has
no unapproved lifetime. Confirmation and notices are separate work.

Only admission creates a run. Pending requests cause no runtime observation,
creation, start or stop. A pending cancellation or stop changes its own intent
to stopped. Cancelling an admitted request is rejected; stopping its game uses
the existing lifecycle. Admission remains occupied until that stop is observed,
then becomes `finished`. Starts while a stop is in progress must wait for its
observed completion. Repeated starts with a current request preserve its
identity, generation and order. Replays retain the existing idempotency result.

Account operations take the account advisory lock and then the installation
advisory lock before server row locks. Global queue maintenance takes only the
installation lock and never requests an account lock while holding it.
Observation holds the existing server advisory lock but no database
transaction spans a runtime effect. Admission is checked before
observation and current generation is checked again before applying an effect.
This does not claim TES-83's broader concurrency acceptance.

Authenticated GET `/v1/start-requests/:id` returns durable state, server,
arrival, valid requests ahead and expiry. POST to the same path plus `/cancel`
uses `client_request_id`. Missing or foreign ownership is 404. Reads and
mutations advance expired reservations atomically in PostgreSQL; the existing
control tick also does this. No new daemon is required. There is no ETA, UI or
polling budget claim.

## Implementation and verification

1. Add independent migration 5 and `src/start-requests.ts` for metadata,
   capacity, FIFO and valid transitions. Migration 4 belongs to TES-88.
2. Connect `database.ts` mutations and observations, `lifecycle.ts` admission
   checks, the existing `index.ts` tick and authenticated `app.ts` routes.
3. Add PostgreSQL tests using synthetic accounts and servers. Restart an actual
   subprocess and verify ten equivalent requests, timestamp ties, replay,
   ownership, cancellation, expiry and slot release after observed stop.
   A shared controlled runtime proves pending stops cannot observe or stop an
   admitted game. HTTP tests cover authentication and response fields.
4. Run root `npm run check` and PostgreSQL regressions. Record results in
   `infra/evidence/tes-79-durable-start-queue.md`.

The fixed Compose container and volume do not establish ten managed worlds.
TES-5 managed-service acceptance, TES-78 confirmation, notification work,
TES-83 full queue concurrency, TES-82 UI and polling budget, warm allocation,
AutoStop and remote backup remain outside this change. TES-80 quota code and
TES-88 backup code are not copied into this branch.
