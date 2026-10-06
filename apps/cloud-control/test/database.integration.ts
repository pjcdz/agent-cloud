import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import test from "node:test";

import pg from "pg";

import { ControlStore } from "../src/database.js";

test("PostgreSQL preserves machine identity, ownership and operation records", async (t) => {
  const connectionString = process.env.TEST_DATABASE_URL;
  assert.ok(connectionString, "TEST_DATABASE_URL must point to a disposable test database");
  const schema = `test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Pool({ connectionString });
  await admin.query(`CREATE SCHEMA ${schema}`);
  const options = { connectionString, options: `-c search_path=${schema}` };
  const pool = new pg.Pool(options);
  let store = new ControlStore(new pg.Pool(options));
  const token = randomUUID();

  try {
    const operator = await store.initialize(token);
    const otherId = randomUUID();
    const serverId = randomUUID();
    const otherServerId = randomUUID();
    const runId = randomUUID();
    await pool.query("INSERT INTO principals (id, subject) VALUES ($1, 'other')", [otherId]);
    for (const [id, owner] of [[serverId, operator.id], [otherServerId, otherId]]) {
      await pool.query(
        `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity)
         VALUES ($1, $2, 'acceptance', 'paper-26.2-121', 'two-player', $3)`,
        [id, owner, id]
      );
    }
    await pool.query("INSERT INTO runs (id, server_id, state) VALUES ($1, $2, 'running')", [runId, serverId]);

    await t.test("authentication stores only a digest and rejects revoked tokens", async () => {
      assert.deepEqual(await store.authenticate(token), operator);
      assert.equal(await store.authenticate(randomUUID()), null);
      const tokens = await pool.query("SELECT octet_length(token_digest) AS bytes FROM machine_tokens");
      assert.deepEqual(tokens.rows, [{ bytes: 32 }]);
      await pool.query("UPDATE machine_tokens SET revoked_at = now()");
      await store.initialize(token);
      assert.equal(await store.authenticate(token), null);
      await pool.query("UPDATE machine_tokens SET revoked_at = NULL");
    });

    await t.test("ownership and one-active-run constraints are enforced by PostgreSQL", async () => {
      await store.assertServerOwnership(operator.id, serverId);
      await pool.query(
        "INSERT INTO events (principal_id, server_id, run_id, event_type) VALUES ($1, $2, $3, 'test')",
        [operator.id, serverId, runId]
      );
      await assert.rejects(store.assertServerOwnership(otherId, serverId), /server_not_found/);
      await assert.rejects(
        pool.query("INSERT INTO runs (id, server_id, state) VALUES ($1, $2, 'queued')", [randomUUID(), serverId]),
        { code: "23505" }
      );
      await assert.rejects(
        pool.query("INSERT INTO events (principal_id, server_id, event_type) VALUES ($1, $2, 'test')", [otherId, serverId]),
        { code: "23503" }
      );
      await assert.rejects(
        pool.query("INSERT INTO events (principal_id, server_id, run_id, event_type) VALUES ($1, $2, $3, 'test')", [otherId, otherServerId, runId]),
        { code: "23503" }
      );
      await assert.rejects(
        pool.query("INSERT INTO events (principal_id, run_id, event_type) VALUES ($1, $2, 'test')", [otherId, runId]),
        { code: "23514" }
      );
    });

    await t.test("mutation claims and completed responses survive a new store", async () => {
      const claims = await Promise.all(Array.from({ length: 10 }, () =>
        store.claimIdempotencyKey(operator.id, "request-1", "create", '{"name":"acceptance"}')
      ));
      assert.equal(claims.filter((claim) => claim.claimed).length, 1);
      await store.completeIdempotencyKey(operator.id, "request-1", 202, { server_id: serverId });
      await store.close();
      store = new ControlStore(new pg.Pool(options));
      assert.deepEqual(await store.initialize(token), operator);
      assert.deepEqual(await store.authenticate(token), operator);
      await store.assertServerOwnership(operator.id, serverId);
      const replay = await store.claimIdempotencyKey(operator.id, "request-1", "create", '{"name":"acceptance"}');
      assert.equal(replay.claimed, false);
      assert.equal(replay.record.responseStatus, 202);
      assert.deepEqual(replay.record.responseBody, { server_id: serverId });
      await assert.rejects(
        store.claimIdempotencyKey(operator.id, "request-1", "create", '{"name":"changed"}'),
        /idempotency_key_reused/
      );
      await assert.rejects(
        store.claimIdempotencyKey(operator.id, "request-1", "stop", '{"name":"acceptance"}'),
        /idempotency_key_reused/
      );
      const migrations = await pool.query("SELECT version FROM schema_migrations ORDER BY version");
      assert.deepEqual(migrations.rows, [{ version: 1 }, { version: 2 }, { version: 3 }, { version: 5 }]);
      assert.equal((await pool.query("SELECT id FROM runs WHERE id = $1", [runId])).rowCount, 1);
      assert.equal((await pool.query("SELECT id FROM events WHERE run_id = $1", [runId])).rowCount, 1);
    });

    await t.test("a configured one-server capacity gate serializes competing creates", async () => {
      const capacityPrincipal = randomUUID();
      await pool.query("INSERT INTO principals (id, subject) VALUES ($1, $2)", [capacityPrincipal, `capacity-${capacityPrincipal}`]);
      const limited = new ControlStore(new pg.Pool(options), 1);
      try {
        const results = await Promise.allSettled([
          limited.mutate(capacityPrincipal, "capacity-one", { operation: "create", name: "one", eula_accepted: true }),
          limited.mutate(capacityPrincipal, "capacity-two", { operation: "create", name: "two", eula_accepted: true })
        ]);
        assert.equal(results.filter(result => result.status === "fulfilled").length, 1);
        const rejected = results.find(result => result.status === "rejected");
        assert.ok(rejected && rejected.status === "rejected");
        assert.equal(rejected.reason.code, "capacity_unavailable");
        assert.equal(rejected.reason.status, 409);
      } finally {
        await limited.close();
      }
    });

    await t.test("low free space rejects a new world without recording the request", async () => {
      const principal = randomUUID();
      await pool.query("INSERT INTO principals (id, subject) VALUES ($1, $2)", [principal, `storage-${principal}`]);
      const full = new ControlStore(new pg.Pool(options), 1, { path: tmpdir(), minFreeBytes: Number.MAX_SAFE_INTEGER });
      const roomy = new ControlStore(new pg.Pool(options), 1, { path: tmpdir(), minFreeBytes: 0 });
      try {
        await assert.rejects(
          full.mutate(principal, "storage-one", { operation: "create", name: "one", eula_accepted: true }),
          { code: "insufficient_storage", status: 507 }
        );
        const counts = await pool.query(
          `SELECT (SELECT count(*) FROM servers WHERE principal_id = $1)::int AS servers,
                  (SELECT count(*) FROM idempotency_keys WHERE principal_id = $1)::int AS keys`, [principal]
        );
        assert.deepEqual(counts.rows, [{ servers: 0, keys: 0 }]);
        const created = await roomy.mutate(principal, "storage-one", { operation: "create", name: "one", eula_accepted: true });
        assert.deepEqual(
          await full.mutate(principal, "storage-one", { operation: "create", name: "one", eula_accepted: true }),
          created
        );
      } finally {
        await full.close();
        await roomy.close();
      }
    });
  } finally {
    await store.close();
    await pool.end();
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    await admin.end();
  }
});
