import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import test from "node:test";

import pg from "pg";

import { BackupStore, type BackupRecord, type BackupState } from "../src/backup.js";
import { ControlStore } from "../src/database.js";
import { migrations } from "../src/migrations.js";

const artifact = { sha256: createHash("sha256").update("synthetic world bytes").digest("hex"), sizeBytes: 21 };

test("TES-88: PostgreSQL preserves backup generations, artifacts and valid states", async (t) => {
  const connectionString = process.env.TEST_DATABASE_URL;
  assert.ok(connectionString, "TEST_DATABASE_URL must point to a disposable test database");
  const schema = `test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Pool({ connectionString });
  await admin.query(`CREATE SCHEMA ${schema}`);
  const options = { connectionString, options: `-c search_path=${schema}` };
  const pool = new pg.Pool(options);
  let store = new ControlStore(new pg.Pool(options));
  const competitor = new BackupStore(pool);
  const token = randomUUID();

  try {
    await pool.query(`CREATE TABLE schema_migrations (
      version integer PRIMARY KEY, name text NOT NULL UNIQUE, applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    for (const migration of migrations.filter(migration => migration.version < 4)) {
      await pool.query(migration.sql);
      await pool.query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", [migration.version, migration.name]);
    }
    const previousOwner = randomUUID();
    const previousServer = randomUUID();
    await pool.query("INSERT INTO principals (id, subject) VALUES ($1, $2)", [previousOwner, previousOwner]);
    await pool.query(
      `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity,
         desired_state, state, generation, observed_generation)
       VALUES ($1, $2, 'existing-world', 'synthetic', 'synthetic', $3, 'running', 'queued', 17, 16)`,
      [previousServer, previousOwner, previousServer]
    );
    const before = (await pool.query("SELECT * FROM servers WHERE id = $1", [previousServer])).rows[0];
    const owner = await store.initialize(token);

    async function server(): Promise<string> {
      const id = randomUUID();
      await pool.query(
        `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity)
         VALUES ($1, $2, $3, 'synthetic', 'synthetic', $3)`, [id, owner.id, id]
      );
      return id;
    }

    await t.test("migration 4 upgrades existing v3 data without changing lifecycle generations", async () => {
      assert.deepEqual((await pool.query("SELECT * FROM servers WHERE id = $1", [previousServer])).rows[0], before);
      assert.deepEqual((await pool.query("SELECT version FROM schema_migrations ORDER BY version")).rows,
        [{ version: 1 }, { version: 2 }, { version: 3 }, { version: 4 }]);
      const backup = await store.backups.createGeneration(previousServer, "first-backup", artifact);
      assert.equal(backup.generation, 1);
      assert.equal(backup.state, "pending");
      assert.equal(backup.object_key, `servers/${previousServer}/backups/1/world`);
      assert.equal(backup.sha256, artifact.sha256);
      assert.equal(backup.size_bytes, String(artifact.sizeBytes));
      assert.deepEqual(backup.created_at, backup.updated_at);
      assert.equal(backup.uploaded_at, null);
      assert.equal(backup.failed_at, null);
      assert.equal(backup.restore_tested_at, null);
      assert.equal(await store.backups.getLatestGood(previousServer), null);
      assert.equal(await store.backups.getGeneration(previousServer, 2), null);
      assert.deepEqual((await pool.query("SELECT * FROM servers WHERE id = $1", [previousServer])).rows[0], before);
    });

    await t.test("competing allocations and request replays serialize across pools per server", async () => {
      const id = await server();
      const stores = [store.backups, competitor];
      const created = await Promise.all(Array.from({ length: 24 }, (_, i) =>
        stores[i % 2]!.createGeneration(id, `race-${i}`, artifact)
      ));
      assert.deepEqual(created.map(row => row.generation).sort((a, b) => a - b),
        Array.from({ length: 24 }, (_, i) => i + 1));
      assert.equal(new Set(created.map(row => row.object_key)).size, 24);
      const replays = await Promise.all(Array.from({ length: 24 }, (_, i) =>
        stores[i % 2]!.createGeneration(id, "one-request", artifact)
      ));
      assert.equal(replays[0]!.generation, 25);
      for (const row of replays) assert.deepEqual(row, replays[0]);
      const count = await pool.query("SELECT count(*)::int AS count FROM backups WHERE server_id = $1", [id]);
      assert.equal(count.rows[0].count, 25);
      const other = await store.backups.createGeneration(await server(), "one-request", artifact);
      assert.equal(other.generation, 1);
      assert.notEqual(other.object_key, replays[0]!.object_key);
    });

    await t.test("a conflicting concurrent replay cannot allocate or replace an artifact", async () => {
      const id = await server();
      const results = await Promise.allSettled([
        store.backups.createGeneration(id, "same-request", artifact),
        competitor.createGeneration(id, "same-request", { ...artifact, sha256: "0".repeat(64) })
      ]);
      assert.equal(results.filter(result => result.status === "fulfilled").length, 1);
      const rejected = results.find(result => result.status === "rejected");
      assert.ok(rejected && rejected.status === "rejected");
      assert.equal(rejected.reason.code, "backup_request_reused");
      const next = await store.backups.createGeneration(id, "next-request", artifact);
      assert.equal(next.generation, 2);
      await assert.rejects(store.backups.createGeneration(id, "next-request", { ...artifact, sizeBytes: 22 }),
        { code: "backup_request_reused", status: 409 });
    });

    await t.test("invalid digests, sizes and requests fail before allocating a generation", async () => {
      const id = await server();
      for (const sha256 of ["", "a".repeat(63), "a".repeat(65), "g".repeat(64), `${artifact.sha256}\n`]) {
        await assert.rejects(store.backups.createGeneration(id, "invalid-digest", { ...artifact, sha256 }),
          { code: "invalid_backup_digest", status: 400 });
      }
      for (const sizeBytes of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        await assert.rejects(store.backups.createGeneration(id, "invalid-size", { ...artifact, sizeBytes }),
          { code: "invalid_backup_size", status: 400 });
      }
      for (const requestId of ["", "contains spaces", "a".repeat(129), "trailing-newline\n"]) {
        await assert.rejects(store.backups.createGeneration(id, requestId, artifact), { code: "invalid_request_id", status: 400 });
      }
      await assert.rejects(store.backups.createGeneration(randomUUID(), "missing-server", artifact),
        { code: "server_not_found", status: 404 });
      const zero = await store.backups.createGeneration(id, "zero", { sha256: artifact.sha256.toUpperCase(), sizeBytes: 0 });
      assert.equal(zero.generation, 1);
      assert.equal(zero.size_bytes, "0");
      assert.equal(zero.sha256, artifact.sha256);
      const large = await store.backups.createGeneration(id, "large", { ...artifact, sizeBytes: Number.MAX_SAFE_INTEGER });
      assert.equal(large.size_bytes, String(Number.MAX_SAFE_INTEGER));
      for (const generation of [0, -1, 1.5, NaN, 2_147_483_648]) {
        await assert.rejects(store.backups.getGeneration(id, generation), { code: "invalid_backup_generation", status: 400 });
      }
      for (const reason of ["", "   ", "a".repeat(1025)]) {
        await assert.rejects(store.backups.markFailed(id, 1, reason), { code: "invalid_backup_failure_reason", status: 400 });
      }
      assert.deepEqual(await store.backups.getGeneration(id, 1), zero);
      await assert.rejects(store.backups.markUploaded(id, 999), { code: "backup_not_found", status: 404 });
    });

    await t.test("PostgreSQL rejects malformed metadata and already-finalized insertions", async () => {
      const id = await server();
      for (const [generation, sha256, size] of [
        [0, artifact.sha256, 21], [1, "invalid", 21], [1, "g".repeat(64), 21],
        [1, artifact.sha256, -1], [1, artifact.sha256, "9007199254740992"]
      ]) {
        await assert.rejects(pool.query(
          "INSERT INTO backups (server_id, generation, request_id, sha256, size_bytes) VALUES ($1, $2, 'invalid', $3, $4)",
          [id, generation, sha256, size]
        ), { code: "23514" });
      }
      await assert.rejects(pool.query(
        `INSERT INTO backups (server_id, generation, request_id, sha256, size_bytes, state, uploaded_at)
         VALUES ($1, 1, 'invalid', $2, 21, 'uploaded', now())`, [id, artifact.sha256]
      ), { code: "23514" });
      await assert.rejects(pool.query(
        "INSERT INTO backups (server_id, generation, request_id, sha256, size_bytes) VALUES ($1, 1, 'bad request', $2, 21)",
        [id, artifact.sha256]
      ), { code: "23514" });
      await assert.rejects(pool.query(
        "INSERT INTO backups (server_id, generation, request_id, sha256, size_bytes) VALUES ($1, 1, 'missing', $2, 21)",
        [randomUUID(), artifact.sha256]
      ), { code: "23503" });
    });

    await t.test("all state transitions and identical retries are enforced by PostgreSQL", async (states) => {
      const valid: Record<BackupState, readonly BackupState[]> = {
        pending: ["pending", "uploaded", "failed"], failed: ["failed", "pending"],
        uploaded: ["uploaded", "restore_tested"], restore_tested: ["restore_tested"]
      };
      async function transition(id: string, generation: number, state: BackupState): Promise<BackupRecord> {
        if (state === "pending") return store.backups.retryGeneration(id, generation);
        if (state === "uploaded") return store.backups.markUploaded(id, generation);
        if (state === "failed") return store.backups.markFailed(id, generation, "synthetic failure");
        return store.backups.markRestoreTested(id, generation);
      }
      for (const from of Object.keys(valid) as BackupState[]) {
        for (const to of Object.keys(valid) as BackupState[]) {
          await states.test(`${from} -> ${to}`, async () => {
            const id = await server();
            let current = await store.backups.createGeneration(id, `${from}-${to}`, artifact);
            if (from === "restore_tested") await store.backups.markUploaded(id, 1);
            if (from !== "pending") current = await transition(id, 1, from);
            if (valid[from].includes(to)) {
              const after = await transition(id, 1, to);
              assert.equal(after.state, to);
              if (from === to) assert.deepEqual(after, current);
              assert.deepEqual(after.created_at, current.created_at);
              assert.ok(after.updated_at >= current.updated_at);
              assert.equal(after.uploaded_at !== null, to === "uploaded" || to === "restore_tested");
              assert.equal(after.failed_at !== null, to === "failed");
              assert.equal(after.restore_tested_at !== null, to === "restore_tested");
              if (after.restore_tested_at !== null) assert.ok(after.restore_tested_at >= after.uploaded_at!);
            } else {
              await assert.rejects(transition(id, 1, to), { code: "invalid_backup_transition", status: 409 });
              await assert.rejects(pool.query(
                "UPDATE backups SET state = $3, failure_reason = $4 WHERE server_id = $1 AND generation = $2",
                [id, 1, to, to === "failed" ? "synthetic failure" : null]
              ), { code: "23514" });
              assert.deepEqual(await store.backups.getGeneration(id, 1), current);
            }
          });
        }
      }
    });

    await t.test("competing uploaded and failed outcomes admit exactly one result", async () => {
      const id = await server();
      await store.backups.createGeneration(id, "completion-race", artifact);
      const results = await Promise.allSettled([
        store.backups.markUploaded(id, 1), competitor.markFailed(id, 1, "synthetic failure")
      ]);
      assert.equal(results.filter(result => result.status === "fulfilled").length, 1);
      const rejected = results.find(result => result.status === "rejected");
      assert.ok(rejected && rejected.status === "rejected");
      assert.equal(rejected.reason.code, "invalid_backup_transition");
      const saved = await store.backups.getGeneration(id, 1);
      assert.ok(saved && (saved.state === "uploaded" || saved.state === "failed"));
      assert.equal(saved.object_key, `servers/${id}/backups/1/world`);
    });

    await t.test("last good artifacts stay immutable through later failures and out-of-order completions", async () => {
      const id = await server();
      const first = await store.backups.createGeneration(id, "good", artifact);
      const good = await store.backups.markUploaded(id, first.generation);
      for (const sql of [
        "sha256 = repeat('0', 64)", "size_bytes = 22", "generation = 100", "request_id = 'replacement'",
        "server_id = '00000000-0000-0000-0000-000000000000'", "created_at = created_at + interval '1 second'",
        "uploaded_at = uploaded_at + interval '1 second'", "updated_at = updated_at + interval '1 second'"
      ]) {
        await assert.rejects(pool.query(`UPDATE backups SET ${sql} WHERE server_id = $1 AND generation = 1`, [id]),
          { code: "23514" });
      }
      await assert.rejects(pool.query("UPDATE backups SET object_key = 'replacement' WHERE server_id = $1", [id]),
        { code: "428C9" });
      await assert.rejects(pool.query("UPDATE backups SET state = 'unknown' WHERE server_id = $1", [id]), { code: "23514" });
      assert.deepEqual(await store.backups.getGeneration(id, 1), good);
      const later = await store.backups.createGeneration(id, "later", { ...artifact, sha256: "0".repeat(64) });
      assert.notEqual(later.object_key, good.object_key);
      assert.deepEqual(await store.backups.getLatestGood(id), good);
      const failed = await store.backups.markFailed(id, 2, "synthetic failure");
      assert.deepEqual(await store.backups.getLatestGood(id), good);
      await assert.rejects(store.backups.markFailed(id, 2, "changed result"), { code: "invalid_backup_transition", status: 409 });
      const retried = await store.backups.retryGeneration(id, 2);
      assert.equal(retried.object_key, failed.object_key);
      assert.equal(retried.sha256, failed.sha256);
      assert.equal(retried.size_bytes, failed.size_bytes);
      assert.equal(retried.failed_at, null);
      assert.equal(retried.failure_reason, null);
      const newer = await store.backups.createGeneration(id, "newer", artifact);
      const newestGood = await store.backups.markUploaded(id, newer.generation);
      await store.backups.markUploaded(id, retried.generation);
      assert.deepEqual(await store.backups.getLatestGood(id), newestGood);
      const proven = await store.backups.markRestoreTested(id, newestGood.generation);
      assert.deepEqual(proven.uploaded_at, newestGood.uploaded_at);
      assert.deepEqual(await store.backups.getLatestGood(id), proven);
      assert.deepEqual(await store.backups.getGeneration(id, 1), good);
    });

    await t.test("durable counter and request replays survive a new store and removal of a failed row", async () => {
      const id = await server();
      await store.backups.createGeneration(id, "persisted", artifact);
      const good = await store.backups.markUploaded(id, 1);
      await store.backups.createGeneration(id, "failed", artifact);
      await store.backups.markFailed(id, 2, "synthetic failure");
      await store.close();
      store = new ControlStore(new pg.Pool(options));
      assert.deepEqual(await store.initialize(token), owner);
      assert.deepEqual(await store.backups.createGeneration(id, "persisted", artifact), good);
      assert.deepEqual(await store.backups.getLatestGood(id), good);
      assert.equal((await store.backups.createGeneration(id, "failed", artifact)).state, "failed");
      await pool.query("DELETE FROM backups WHERE server_id = $1 AND generation = 2", [id]);
      const next = await store.backups.createGeneration(id, "after-retention", artifact);
      assert.equal(next.generation, 3);
      assert.notEqual(next.object_key, `servers/${id}/backups/2/world`);
      assert.deepEqual(await store.backups.getLatestGood(id), good);
    });
  } finally {
    await store.close();
    await pool.end();
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    await admin.end();
  }
});
