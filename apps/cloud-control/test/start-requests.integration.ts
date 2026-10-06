import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import test from "node:test";
import pg from "pg";

import { ControlStore } from "../src/database.js";
import { Reconciler, type AcceptedMutation, type Runtime, type RuntimeObservation, type ServerRecord } from "../src/lifecycle.js";
import type { StartRequestStatus } from "../src/start-requests.js";
import type { QueueCommand } from "./fixtures/queue-process.js";

class SharedRuntime implements Runtime {
  public observation: RuntimeObservation = { state: "stopped" };
  public readonly effects: Array<{ server: string; effect: keyof Runtime }> = [];
  public beforeObserve: (() => Promise<void>) | undefined;
  public constructor(private readonly check: () => Promise<void>) {}
  private async record(server: ServerRecord, effect: keyof Runtime): Promise<void> {
    await this.check();
    this.effects.push({ server: server.id, effect });
  }
  public async observe(server: ServerRecord): Promise<RuntimeObservation> {
    await this.record(server, "observe");
    const observed = this.observation;
    await this.beforeObserve?.();
    return observed;
  }
  public async create(server: ServerRecord): Promise<void> { await this.record(server, "create"); }
  public async start(server: ServerRecord): Promise<void> {
    await this.record(server, "start");
    this.observation = { state: "ready", endpoint: { host: "game.invalid", port: 25565 } };
  }
  public async stop(server: ServerRecord): Promise<void> {
    await this.record(server, "stop");
    this.observation = { state: "stopping" };
  }
}

async function queueProcess(schema: string, token: string): Promise<{
  child: ChildProcess; call: <T>(command: QueueCommand) => Promise<T>;
}> {
  const child = fork(new URL("./fixtures/queue-process.js", import.meta.url), [schema, token],
    { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += String(chunk); });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error(`queue process timed out: ${stderr}`)); }, 15000);
    child.once("error", reject);
    child.once("message", message => {
      clearTimeout(timer);
      assert.deepEqual(message, { ready: true });
      resolve();
    });
    child.once("exit", code => { clearTimeout(timer); if (code !== null) reject(new Error(`queue process exited: ${stderr}`)); });
  });
  return { child, call: <T>(command: QueueCommand) => new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`queue command timed out: ${stderr}`)), 15000);
    child.once("message", message => {
      clearTimeout(timer);
      const response = message as { result: T; error?: string };
      if (response.error !== undefined) reject(new Error(response.error));
      else resolve(response.result);
    });
    child.send(command);
  }) };
}

async function killProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}

test("TES-79: durable FIFO admission survives a control-process restart", async (t) => {
  const connectionString = process.env.TEST_DATABASE_URL;
  assert.ok(connectionString, "TEST_DATABASE_URL must point to a disposable test database");
  const schema = `test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Pool({ connectionString });
  await admin.query(`CREATE SCHEMA ${schema}`);
  const options = { connectionString, options: `-c search_path=${schema}`, application_name: schema };
  const inspection = new pg.Pool({ ...options, application_name: `${schema}_inspection` });
  const store = new ControlStore(new pg.Pool(options), 0, null, true);
  const children: ChildProcess[] = [];
  const token = randomUUID();
  const accounts: Array<{ principalId: string; serverId: string }> = [];
  const accepted: AcceptedMutation[] = [];
  const turns: string[] = [];

  async function account(profile = "two-player"): Promise<{ principalId: string; serverId: string }> {
    const principalId = randomUUID();
    const serverId = randomUUID();
    await inspection.query("INSERT INTO principals (id, subject) VALUES ($1, $2)", [principalId, principalId]);
    await inspection.query(
      `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity)
       VALUES ($1, $2, 'one', 'paper-26.2-121', $3, $4)`, [serverId, principalId, profile, `world-${serverId}`]
    );
    return { principalId, serverId };
  }

  const runtime = new SharedRuntime(async () => {
    const result = await inspection.query<{ count: string }>(
      "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle in transaction'", [schema]
    );
    assert.equal(result.rows[0]!.count, "0", "runtime effects never hold an open database transaction");
  });
  const reconciler = new Reconciler(store, runtime);

  try {
    const owner = await store.initialize(token);
    const running = await store.mutate(owner.id, "occupy-slot", { operation: "create", name: "one", eula_accepted: true });
    assert.ok(running.start_request_id);
    await t.test("free capacity admits once with the independent migration 5", async () => {
      const versions = await inspection.query("SELECT version FROM schema_migrations ORDER BY version");
      assert.deepEqual(versions.rows, [{ version: 1 }, { version: 2 }, { version: 3 }, { version: 5 }]);
      const request = await store.getStartRequest(owner.id, running.start_request_id!);
      assert.equal(request.state, "admitted");
      assert.equal(request.expires_at, null);
      assert.equal(running.state, "provisioning");
      await reconciler.reconcile(running.server_id);
      await reconciler.reconcile(running.server_id);
      assert.equal((await store.getServer(owner.id, running.server_id)).state, "running");
      assert.equal(runtime.effects.filter(effect => effect.effect === "start").length, 1);
    });

    await t.test("ten equivalent requests retain identity and FIFO when timestamps tie and the process is killed", async () => {
      for (let i = 0; i < 10; i++) accounts.push(await account());
      const first = await queueProcess(schema, token);
      children.push(first.child);
      for (const [i, item] of accounts.entries()) {
        accepted.push(await first.call<AcceptedMutation>({ action: "mutate", principalId: item.principalId,
          requestId: `start-${i}`, mutation: { operation: "start", server_id: item.serverId } }));
      }
      await inspection.query("UPDATE start_requests SET arrived_at = '2026-10-06T12:00:00Z' WHERE state = 'queued'");
      const before: StartRequestStatus[] = [];
      for (const [i, item] of accounts.entries()) {
        before.push(await first.call<StartRequestStatus>({ action: "get", principalId: item.principalId,
          requestId: accepted[i]!.start_request_id! }));
        assert.equal(before[i]!.state, "queued");
        assert.equal(before[i]!.valid_requests_ahead, i);
        assert.equal(before[i]!.expires_at, null);
        if (i > 0) assert.ok(BigInt(before[i]!.arrival_order) > BigInt(before[i - 1]!.arrival_order));
      }
      await killProcess(first.child);
      const second = await queueProcess(schema, token);
      children.push(second.child);
      assert.notEqual(first.child.pid, second.child.pid);
      for (const [i, item] of accounts.entries()) {
        const after = await second.call<StartRequestStatus>({ action: "get", principalId: item.principalId,
          requestId: accepted[i]!.start_request_id! });
        assert.deepEqual(after, before[i]);
        assert.deepEqual(await second.call<AcceptedMutation>({ action: "mutate", principalId: item.principalId,
          requestId: `start-${i}`, mutation: { operation: "start", server_id: item.serverId } }), accepted[i]);
      }
      await killProcess(second.child);
      const runs = await inspection.query("SELECT count(*) FROM runs WHERE server_id = ANY($1::uuid[])",
        [accounts.map(item => item.serverId)]);
      assert.equal(runs.rows[0].count, "0");
      const effects = runtime.effects.length;
      for (const item of accounts) await reconciler.reconcile(item.serverId);
      assert.equal(runtime.effects.length, effects);
    });

    await t.test("repeated start preserves generation and place; conflicting keys and foreign ownership fail", async () => {
      const item = accounts[0]!;
      const before = await store.getServer(item.principalId, item.serverId);
      const repeated = await store.mutate(item.principalId, "start-again", { operation: "start", server_id: item.serverId });
      assert.equal(repeated.start_request_id, accepted[0]!.start_request_id);
      assert.equal((await store.getServer(item.principalId, item.serverId)).generation, before.generation);
      assert.equal((await store.getStartRequest(item.principalId, repeated.start_request_id!)).valid_requests_ahead, 0);
      await assert.rejects(store.mutate(item.principalId, "start-0", { operation: "stop", server_id: item.serverId }),
        { code: "idempotency_key_reused", status: 409 });
      await assert.rejects(store.getStartRequest(owner.id, repeated.start_request_id!),
        { code: "start_request_not_found", status: 404 });
      await assert.rejects(store.cancelStartRequest(owner.id, repeated.start_request_id!, "foreign-cancel"),
        { code: "start_request_not_found", status: 404 });
      await assert.rejects(store.mutate(owner.id, "foreign-start", { operation: "start", server_id: item.serverId }),
        { code: "server_not_found", status: 404 });
      const unsupported = await account("unqualified");
      await assert.rejects(store.mutate(unsupported.principalId, "bad-profile", { operation: "start", server_id: unsupported.serverId }),
        { code: "profile_not_qualified", status: 409 });
      assert.equal((await inspection.query("SELECT count(*) FROM start_requests WHERE server_id = $1", [item.serverId])).rows[0].count, "1");
    });

    await t.test("cancelling or stopping pending B never observes or stops admitted A on the shared runtime", async () => {
      const cancelled = await account();
      const stopped = await account();
      const pending = await store.mutate(cancelled.principalId, "cancelled-start", { operation: "start", server_id: cancelled.serverId });
      await store.mutate(stopped.principalId, "stopped-start", { operation: "start", server_id: stopped.serverId });
      const baseline = runtime.effects.length;
      const response = await store.cancelStartRequest(cancelled.principalId, pending.start_request_id!, "cancel-b");
      assert.deepEqual(await store.cancelStartRequest(cancelled.principalId, pending.start_request_id!, "cancel-b"), response);
      await store.mutate(stopped.principalId, "stop-b", { operation: "stop", server_id: stopped.serverId });
      for (const item of [cancelled, stopped]) {
        await reconciler.reconcile(item.serverId);
        const server = await store.getServer(item.principalId, item.serverId);
        assert.equal(server.state, "stopped");
        assert.equal(server.desired_state, "stopped");
      }
      assert.equal(runtime.effects.length, baseline);
      assert.equal((await store.getStartRequest(owner.id, running.start_request_id!)).state, "admitted");
      assert.equal((await store.getServer(owner.id, running.server_id)).desired_state, "running");
      await assert.rejects(store.cancelStartRequest(owner.id, running.start_request_id!, "cancel-active"),
        { code: "start_request_not_pending", status: 409 });
      await assert.rejects(store.cancelStartRequest(cancelled.principalId, accepted[1]!.start_request_id!, "cancel-b"),
        { code: "idempotency_key_reused", status: 409 });
    });

    await t.test("a newer stop fences an older observation and retains capacity until the actual stop", async () => {
      runtime.beforeObserve = async () => {
        await store.mutate(owner.id, "stop-a", { operation: "stop", server_id: running.server_id });
      };
      const baseline = runtime.effects.filter(effect => effect.effect !== "observe").length;
      await reconciler.reconcile(running.server_id);
      runtime.beforeObserve = undefined;
      assert.equal(runtime.effects.filter(effect => effect.effect !== "observe").length, baseline);
      assert.equal((await store.getStartRequest(owner.id, running.start_request_id!)).state, "admitted");
      assert.equal((await store.getStartRequest(accounts[0]!.principalId, accepted[0]!.start_request_id!)).state, "queued");
      await assert.rejects(store.mutate(owner.id, "restart-stopping", { operation: "start", server_id: running.server_id }),
        { code: "server_stopping", status: 409 });
      await reconciler.reconcile(running.server_id);
      assert.equal(runtime.observation.state, "stopping");
      assert.equal((await store.getStartRequest(owner.id, running.start_request_id!)).state, "admitted");
      runtime.observation = { state: "stopped" };
      await reconciler.reconcile(running.server_id);
      const finished = await store.getStartRequest(owner.id, running.start_request_id!);
      assert.equal(finished.state, "finished");
      assert.ok(finished.terminal_at);
      const head = await store.getStartRequest(accounts[0]!.principalId, accepted[0]!.start_request_id!);
      assert.equal(head.state, "reserved");
      assert.equal(head.expires_at!.getTime() - head.reserved_at!.getTime(), 300_000);
      const effects = runtime.effects.length;
      await reconciler.reconcile(accounts[0]!.serverId);
      assert.equal(runtime.effects.length, effects);
    });

    await t.test("cancellation and database expiry advance all ten FIFO turns without confirmation or a daemon", async () => {
      const first = accounts[0]!;
      turns.push(accepted[0]!.start_request_id!);
      const cancelled = await store.cancelStartRequest(first.principalId, accepted[0]!.start_request_id!, "cancel-head");
      const terminal = (await store.getStartRequest(first.principalId, accepted[0]!.start_request_id!)).terminal_at;
      assert.deepEqual(await store.cancelStartRequest(first.principalId, accepted[0]!.start_request_id!, "cancel-head-again"), cancelled);
      assert.deepEqual((await store.getStartRequest(first.principalId, accepted[0]!.start_request_id!)).terminal_at, terminal);
      const second = accounts[1]!;
      assert.equal((await store.getStartRequest(second.principalId, accepted[1]!.start_request_id!)).state, "reserved");
      turns.push(accepted[1]!.start_request_id!);
      await inspection.query(
        `UPDATE start_requests SET reserved_at = statement_timestamp() - interval '6 minutes',
           expires_at = statement_timestamp() - interval '1 minute' WHERE id = $1`, [accepted[1]!.start_request_id]
      );
      assert.equal((await store.getStartRequest(second.principalId, accepted[1]!.start_request_id!)).state, "expired");
      assert.equal((await store.getServer(second.principalId, second.serverId)).desired_state, "stopped");
      const tail = await store.mutate(second.principalId, "start-after-expiry", { operation: "start", server_id: second.serverId });
      assert.notEqual(tail.start_request_id, accepted[1]!.start_request_id);
      assert.equal((await store.getStartRequest(second.principalId, tail.start_request_id!)).valid_requests_ahead, 8);
      const effects = runtime.effects.length;
      for (let i = 2; i < 10; i++) {
        const item = accounts[i]!;
        const request = await store.getStartRequest(item.principalId, accepted[i]!.start_request_id!);
        assert.equal(request.state, "reserved");
        assert.equal(request.valid_requests_ahead, 0);
        turns.push(request.id);
        await reconciler.reconcile(item.serverId);
        await store.cancelStartRequest(item.principalId, request.id, `cancel-turn-${i}`);
        await reconciler.reconcile(item.serverId);
      }
      assert.deepEqual(turns, accepted.map(request => request.start_request_id));
      assert.equal(runtime.effects.length, effects);
      assert.equal((await store.getStartRequest(second.principalId, tail.start_request_id!)).state, "reserved");
      await store.cancelStartRequest(second.principalId, tail.start_request_id!, "cancel-tail");
    });

    await t.test("opting in retains and stops a prior foundation run without metadata or extra capacity", async () => {
      const legacy = new ControlStore(new pg.Pool(options));
      try {
        const prior = await legacy.mutate(owner.id, "legacy-create", { operation: "create", name: "legacy", eula_accepted: true });
        const waiting = await account();
        const queued = await store.mutate(waiting.principalId, "legacy-wait", { operation: "start", server_id: waiting.serverId });
        assert.equal((await store.getStartRequest(waiting.principalId, queued.start_request_id!)).state, "queued");
        const effects = runtime.effects.filter(effect => effect.effect !== "observe").length;
        await reconciler.reconcile(prior.server_id);
        assert.equal(runtime.effects.filter(effect => effect.effect !== "observe").length, effects);
        runtime.observation = { state: "ready" };
        await store.mutate(owner.id, "legacy-stop", { operation: "stop", server_id: prior.server_id });
        await reconciler.reconcile(prior.server_id);
        assert.equal(runtime.observation.state, "stopping");
        assert.equal((await store.getStartRequest(waiting.principalId, queued.start_request_id!)).state, "queued");
        runtime.observation = { state: "stopped" };
        await reconciler.reconcile(prior.server_id);
        assert.equal((await store.getServer(owner.id, prior.server_id)).state, "stopped");
        assert.equal((await store.getStartRequest(waiting.principalId, queued.start_request_id!)).state, "reserved");
        await store.cancelStartRequest(waiting.principalId, queued.start_request_id!, "legacy-cancel-wait");

        const adopted = await legacy.mutate(owner.id, "adopt-create", { operation: "create", name: "adopt", eula_accepted: true });
        runtime.observation = { state: "ready" };
        const repeated = await store.mutate(owner.id, "adopt-start", { operation: "start", server_id: adopted.server_id });
        assert.ok(repeated.start_request_id);
        assert.equal((await store.getStartRequest(owner.id, repeated.start_request_id!)).state, "admitted");
        assert.equal((await inspection.query("SELECT count(*) FROM runs WHERE server_id = $1 AND finished_at IS NULL",
          [adopted.server_id])).rows[0].count, "1");
        const stops = runtime.effects.filter(effect => effect.effect === "stop").length;
        await store.mutate(owner.id, "adopt-stop", { operation: "stop", server_id: adopted.server_id });
        await reconciler.reconcile(adopted.server_id);
        runtime.observation = { state: "stopped" };
        await reconciler.reconcile(adopted.server_id);
        assert.equal(runtime.effects.filter(effect => effect.effect === "stop").length, stops + 1);
        assert.equal((await store.getStartRequest(owner.id, repeated.start_request_id!)).state, "finished");
      } finally { await legacy.close(); }
    });
  } finally {
    for (const child of children) await killProcess(child);
    await store.close();
    await inspection.end();
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    await admin.end();
  }
});
