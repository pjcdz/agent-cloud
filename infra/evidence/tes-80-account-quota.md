# TES-80: atomic managed-beta account quota

The approved D3 rule allows one logical server and one active server per
account. `CLOUD_ACCOUNT_QUOTA=beta` enables this policy in `cloud-control`.
It is independent of the Compose installation's capacity setting.

Create and start acquire a transaction-scoped PostgreSQL lock for the
principal before checking quota and recording intent. Excess returns
`409 quota_exceeded`. An accepted request replay retains its recorded result.
A stopped logical server still occupies its logical slot; an unfinished run
continues occupying the active slot during shutdown. Starting the same server
again does not consume another slot.

## Verification on 2026-10-06

Base: public `main` commit `f09832c78ce154438e4015cc55e7ac7301781228`.
The implementation and tests accompany this record in the same commit.

Environment: macOS 26.6.2 arm64, Node 24.14.1, npm 11.11.0, PostgreSQL 17.11
from the locally available `postgres:17` image. PostgreSQL used a new temporary
container with RAM-backed data, an ephemeral loopback port and random test
credentials. The container was stopped after verification.

| Command | Result |
| --- | --- |
| `npm run check` | 26 passed, 0 failed |
| `npm run test:integration` | 30 passed, 0 failed |
| `bash infra/compose/test.sh` | Passed |

The new integration cases use two independent database pools. Twenty distinct
concurrent creates admit one logical server and one active run; all nineteen
losers receive `quota_exceeded`, and their requests and events roll back.
Accepted replay, changed-body conflict, independent account limits and
repeated starts are checked against persisted records. Pre-policy fixtures
with two offline logical servers exercise the active limit independently:
one competing start wins, the other is rejected until the winner's stop is
observed, and then a retry can start. These fixtures do not claim that newly
created beta accounts may own two logical servers.

The first integration attempt had a UUID/text parameter error in that fixture;
the fixture was corrected and the entire integration suite passed on a new
temporary database. The existing `capacity_unavailable` and restart/crash
recovery tests also passed. A separate read-only review found no blocking
issue in the quota diff.

These checks prove the account admission contract under concurrent requests.
They do not prove managed identity, HTTP edge protection, additional game
capacity, a queue, live Minecraft play or invited-beta acceptance. The local
Compose quickstart leaves the beta quota setting unset and keeps its existing
one-installation capacity result. No production account, credential, world,
deployment or permission was changed.
