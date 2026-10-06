import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import pg from "pg";

import { ControlStore } from "../src/database.js";

test("TES-80: PostgreSQL serializes the approved beta account quota", async (t) => {
  const connectionString = process.env.TEST_DATABASE_URL;
  assert.ok(connectionString, "TEST_DATABASE_URL must point to a disposable test database");
  const schema = `test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Pool({ connectionString });
  await admin.query(`CREATE SCHEMA ${schema}`);
  const options = { connectionString, options: `-c search_path=${schema}` };
  const inspection = new pg.Pool(options);
  const stores = [0, 1].map(() => new ControlStore(new pg.Pool(options), 0, null, true));

  async function account(): Promise<string> {
    const id = randomUUID();
    await inspection.query("INSERT INTO principals (id, subject) VALUES ($1, $2)", [id, `quota-${id}`]);
    return id;
  }

  async function counts(principalId: string) {
    const result = await inspection.query(
      `SELECT (SELECT count(*) FROM servers WHERE principal_id = $1)::int AS logical,
              (SELECT count(*) FROM runs JOIN servers ON servers.id = runs.server_id
                 WHERE principal_id = $1 AND finished_at IS NULL)::int AS active`, [principalId]
    );
    return result.rows[0];
  }

  async function observe(serverId: string, state: "ready" | "stopped") {
    await stores[0]!.withServerLock(serverId, async client => {
      const server = await stores[0]!.getDesiredServer(serverId, client);
      assert.ok(server);
      await stores[0]!.recordObservation(server, { state }, client);
    });
  }

  try {
    await stores[0]!.initialize(randomUUID());

    await t.test("competing creates admit one server and return quota_exceeded to every loser", async () => {
      const owner = await account();
      const results = await Promise.allSettled(Array.from({ length: 20 }, (_, i) =>
        stores[i % 2]!.mutate(owner, `create-${i}`, {
          operation: "create", name: `server-${i}`, eula_accepted: true
        })
      ));
      const winners = results.filter(result => result.status === "fulfilled");
      assert.equal(winners.length, 1);
      for (const result of results) {
        if (result.status === "rejected") {
          assert.equal(result.reason.code, "quota_exceeded");
          assert.equal(result.reason.status, 409);
        }
      }
      assert.deepEqual(await counts(owner), { logical: 1, active: 1 });
      const records = await inspection.query(
        `SELECT (SELECT count(*) FROM idempotency_keys WHERE principal_id = $1)::int AS keys,
                (SELECT count(*) FROM events WHERE principal_id = $1)::int AS events`, [owner]
      );
      assert.deepEqual(records.rows, [{ keys: 1, events: 1 }]);
      const winner = winners[0]!;
      assert.equal(winner.status, "fulfilled");
      const accepted = winner.value;
      const name = (await stores[0]!.getServer(owner, accepted.server_id)).logical_name;
      assert.deepEqual(await stores[1]!.mutate(owner, accepted.request_id, {
        operation: "create", name, eula_accepted: true
      }), accepted);
      await assert.rejects(stores[1]!.mutate(owner, accepted.request_id, {
        operation: "create", name: "different", eula_accepted: true
      }), { code: "idempotency_key_reused", status: 409 });
      await stores[0]!.mutate(owner, "stop", { operation: "stop", server_id: accepted.server_id });
      await observe(accepted.server_id, "stopped");
      assert.deepEqual(await counts(owner), { logical: 1, active: 0 });
      await assert.rejects(stores[1]!.mutate(owner, "replacement", {
        operation: "create", name: "replacement", eula_accepted: true
      }), { code: "quota_exceeded", status: 409 });
    });

    await t.test("accounts have independent quotas and repeated starts retain one active run", async () => {
      const owners = await Promise.all([account(), account()]);
      const accepted = await Promise.all(owners.map((owner, i) => stores[i]!.mutate(owner, "create", {
        operation: "create", name: "minecraft", eula_accepted: true
      })));
      for (const [i, owner] of owners.entries()) {
        await Promise.all(Array.from({ length: 10 }, (_, j) => stores[j % 2]!.mutate(owner, `start-${j}`, {
          operation: "start", server_id: accepted[i]!.server_id
        })));
        assert.deepEqual(await counts(owner), { logical: 1, active: 1 });
      }
      await assert.rejects(stores[0]!.mutate(owners[0]!, "foreign", {
        operation: "start", server_id: accepted[1]!.server_id
      }), { code: "server_not_found", status: 404 });
    });

    await t.test("starts on pre-existing offline servers compete for one active slot until stop is observed", async () => {
      const owner = await account();
      const serverIds = [randomUUID(), randomUUID()];
      // Older installations may have records created before account quotas were enabled.
      for (const [i, id] of serverIds.entries()) {
        await inspection.query(
          `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity)
           VALUES ($1, $2, $3, 'paper-26.2-121', 'two-player', $4)`,
          [id, owner, `existing-${i}`, `cloud-world-${id}`]
        );
      }
      const results = await Promise.allSettled(serverIds.map((serverId, i) => stores[i]!.mutate(owner, `start-${i}`, {
        operation: "start", server_id: serverId
      })));
      assert.equal(results.filter(result => result.status === "fulfilled").length, 1);
      const winnerIndex = results.findIndex(result => result.status === "fulfilled");
      const loserIndex = 1 - winnerIndex;
      const loser = results[loserIndex]!;
      assert.equal(loser.status, "rejected");
      assert.equal(loser.reason.code, "quota_exceeded");
      assert.equal(loser.reason.status, 409);
      assert.deepEqual(await counts(owner), { logical: 2, active: 1 });
      const winnerId = serverIds[winnerIndex]!;
      const loserId = serverIds[loserIndex]!;
      await stores[0]!.mutate(owner, "stop-active", { operation: "stop", server_id: winnerId });
      await observe(winnerId, "ready");
      await assert.rejects(stores[1]!.mutate(owner, `start-${loserIndex}`, {
        operation: "start", server_id: loserId
      }), { code: "quota_exceeded", status: 409 });
      await observe(winnerId, "stopped");
      assert.deepEqual(await counts(owner), { logical: 2, active: 0 });
      await stores[1]!.mutate(owner, `start-${loserIndex}`, { operation: "start", server_id: loserId });
      assert.deepEqual(await counts(owner), { logical: 2, active: 1 });
    });
  } finally {
    await Promise.all(stores.map(store => store.close()));
    await inspection.end();
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    await admin.end();
  }
});
