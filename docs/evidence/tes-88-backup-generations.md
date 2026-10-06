# TES-88 backup metadata

[TES-88](https://linear.app/testingasdad/issue/TES-88/e411-add-backup-generations-and-states)
adds database metadata for generation-based world backups. It belongs to
[TES-37](https://linear.app/testingasdad/issue/TES-37/e41-world-backup-and-local-restore).
Uploading, R2 access, manual backup and world restore remain TES-89/90/91 work.
This change records outcomes supplied by those callers; it performs no storage
operation or world verification and does not complete TES-37.

## Contract

Migration 4, `backup_generations_and_states`, adds `backups` and
`backup_generation_counters`. Both are regular PostgreSQL tables and must be
included in control database backup and recovery. The counter is independent of
the server's lifecycle intent revision in `servers.generation`.

`ControlStore.backups` shares the control store's pool. `BackupStore` and its types
are exported from `apps/cloud-control/src/backup.ts`.

| Method | Result |
| --- | --- |
| `createGeneration(serverId, requestId, {sha256, sizeBytes})` | Creates pending metadata, or returns the existing generation for the same request and artifact. |
| `getGeneration(serverId, generation)` | Returns the row or `null`. |
| `getLatestGood(serverId)` | Returns the greatest uploaded or restore-tested generation, or `null`. |
| `markUploaded(serverId, generation)` | Records the caller's successful upload outcome. |
| `markFailed(serverId, generation, reason)` | Records a failed outcome with a nonempty reason. |
| `retryGeneration(serverId, generation)` | Returns a failed generation to pending for the same artifact and key. |
| `markRestoreTested(serverId, generation)` | Records the caller's successful restore-test outcome. |

SHA-256 must contain exactly 64 hexadecimal characters and is stored as lowercase
text. `sizeBytes` must be a nonnegative safe integer. Returned `size_bytes` is a
decimal string, following node-postgres's bigint representation. Timestamp fields
are returned as `Date` values or `null`.

Creation locks the server's counter row and commits the increment with the metadata
in one transaction. Concurrent requests receive distinct increasing generations.
A replay with different SHA-256 or size fails with `backup_request_reused`.
The durable counter also prevents generation reuse if failed metadata is later
removed; this change adds no retention or deletion API.

PostgreSQL generates `servers/<server UUID>/backups/<generation>/world` as the
object key. A retry preserves the same artifact. New bytes require a new request
and generation. The schema protects server, generation, request, digest, size,
created timestamp and generated key from updates.

| State | Allowed next states |
| --- | --- |
| `pending` | `uploaded`, `failed` |
| `failed` | `pending` |
| `uploaded` | `restore_tested` |
| `restore_tested` | Terminal |

An identical repeat of the current state returns the same timestamps and metadata.
A repeated failed result must preserve its reason. A PostgreSQL trigger enforces
the graph even for direct SQL writes and sets transition timestamps. The API
reports invalid transitions as `invalid_backup_transition` with status 409.

Later pending or failed generations do not displace the last good generation.
Out-of-order successful completions are selected by generation, not completion
time. Uploaded metadata is distinct from a proven world restore; the caller must
verify the corresponding operation before recording either outcome.

## Verification

Verification date: 2026-10-06. Implementation base:
`f09832c78ce154438e4015cc55e7ac7301781228`.

```sh
npm run check
TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_URL" npm run test:integration
```

- `npm run check`: 25 passed, 0 failed.
- Full PostgreSQL integration: 52 passed, 0 failed, including 26 TES-88 tests.

The integration suite uses PostgreSQL 17.11 in an isolated disposable local
container with synthetic data. `backup.integration.ts` exercises all 16 state
pairs, concurrent allocations and replays across pools, conflicting replay
artifacts, competing uploaded/failed outcomes, digest and size validation in the
API and database, immutable metadata and object keys, later failures and
out-of-order completions, a new store reading persisted metadata, counter
durability after removing a failed row, and an additive v3-to-v4 migration that
preserves existing lifecycle metadata.

Existing authentication, idempotency, lifecycle and process crash-recovery tests
also run. These tests validate metadata only; R2 and Minecraft restore acceptance
remain outside TES-88.
