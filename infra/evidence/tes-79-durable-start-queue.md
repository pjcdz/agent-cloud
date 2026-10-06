# TES-79 durable start queue evidence

Date: 2026-10-06

Branch: `codex/tes-79-durable-start-queue`

Base: `f09832c78ce154438e4015cc55e7ac7301781228`, refreshed `origin/main`
before creating the isolated worktree.

## Implemented contract

The [design](../../docs/plans/2026-10-06-tes-79-durable-start-queue.md) records
the current TES-79 criterion and the approved contract. Optional
`CLOUD_START_QUEUE=beta` enables backend metadata and admission for one fixed
Paper slot. Migration 5 works independently of TES-88 migration 4. No account
quota or backup implementation is included in this branch.

Durable requests record owner, server, intent generation, database arrival,
FIFO order, state, reservation, expiry, admission and terminal timestamps.
Only admission creates a run. A new arrival cannot bypass earlier requests.
The oldest queued request receives a five-minute reservation when capacity
becomes available; confirmation remains separate work. Database operations
and the existing control tick process expiry.

Authenticated backend GET and cancel routes expose state for later polling.
Cancel and stop of a pending request cannot observe or stop the shared runtime.
A requested stop retains capacity until actual termination is observed; that
observation finishes the admitted request and releases the slot. Prior
foundation runs remain observable and stoppable after opting in. A repeated
start can adopt such a run without creating a second execution.

## Verification

| Check | Result |
| --- | --- |
| `npm run check` | TypeScript build and 27 tests passed; 0 failed or skipped. |
| `TEST_DATABASE_URL='<disposable database>' npm run test:integration` | PostgreSQL 17.11; all 34 tests passed; 0 failed or skipped. |
| Focused `start-requests.integration.js` | 8 tests passed, including the parent test; 0 failed or skipped. |
| `git diff --check` | Passed. |
| Compose configuration, root lockfile, benchmark package and lockfile | Unchanged. |

PostgreSQL used an existing local image in a new task-owned container with
tmpfs storage and a loopback-only ephemeral port. No existing container,
volume, world or credentials were used. Test schemas and data were synthetic
and removed by their test cleanup. No dependency download or paid API was
needed.

The FIFO test creates ten separate equivalent server requests behind one
occupied slot. It assigns identical arrival timestamps, records all identities,
orders, states and ahead counts, kills the control subprocess with `SIGKILL`,
starts a different process and compares the complete records and idempotent
responses. Cancellation and expiry then produce all ten turns in their
original FIFO order. Pending requests create zero runs and produce zero
runtime effects, including while reserved or after cancellation.

Directed checks also cover repeated-start generation preservation, request-key
conflicts, foreign ownership, rejection of an unqualified profile, idempotent
cancellation, reservation advancement from a database read without a daemon,
and a new attempt joining the tail. A shared controlled runtime proves that
cancelling or stopping pending B leaves admitted A untouched. A stale
observation after a newer stop intent cannot apply an old generation; the
admission remains until the subsequent observed stop. Legacy-run checks cover
both stopping without request metadata and repeated-start adoption followed
by one convergent stop. Runtime calls assert that no database transaction
spans an external effect.

## Retained initial failures

The first root-check attempt was blocked by sandbox `EPERM` on local HTTP
listeners. The authorized rerun with local sockets passed all 27 tests.

The first focused PostgreSQL run failed in synthetic fixture setup with
`42P08`: one SQL parameter was used as both UUID and text. Separate fixture
parameters corrected that mismatch without changing the expected behavior.
The focused rerun and full PostgreSQL suite passed. Initial and final logs
were retained in the task workspace for review.

## Acceptance limits

This verifies durable backend FIFO across a real process restart using
PostgreSQL and controlled runtime effects. It does not qualify ten live
worlds on the fixed Compose container or a managed installation. TES-5
managed-service acceptance, TES-78 confirmation, notification work, TES-82 UI
and polling budget, TES-83 full queue concurrency acceptance, warm allocation,
AutoStop and remote backup remain separate. The conservative one-slot gate
does not measure physical resources or infer extra capacity from the 4 GB
storage soft threshold.
