import { createHash, randomUUID } from "node:crypto";
import { statfs } from "node:fs/promises";

import pg from "pg";

import { authenticateMachineToken, bootstrapOperator, type Principal } from "./auth.js";
import { migrate } from "./migrations.js";
import { ControlError, type AcceptedMutation, type Mutation, type RuntimeObservation, type ServerRecord, type ServerState } from "./lifecycle.js";
import { advanceStartQueue, cancelStartRequest, currentStartRequest, lockStartQueue,
  readStartRequest, recordStartRequest, type StartRequestRecord, type StartRequestStatus } from "./start-requests.js";

const { Pool } = pg;

export interface IdempotencyRecord {
  readonly operation: string;
  readonly requestDigest: Buffer;
  readonly responseStatus: number | null;
  readonly responseBody: unknown;
}

export interface StorageReserve {
  readonly path: string;
  readonly minFreeBytes: number;
}

// Refuses new worlds before the filesystem holding them drops below the reserve.
export async function assertFreeSpace(
  reserve: StorageReserve | null,
  stat: (path: string) => Promise<{ bavail: number; bsize: number }> = statfs
): Promise<void> {
  if (reserve === null) return;
  const stats = await stat(reserve.path);
  if (stats.bavail * stats.bsize < reserve.minFreeBytes) throw new ControlError("insufficient_storage", 507);
}

export class ControlStore {
  public constructor(
    private readonly pool: pg.Pool,
    private readonly maxServers = 0,
    private readonly storage: StorageReserve | null = null,
    private readonly startQueue = false
  ) {}

  public static fromEnvironment(environment: NodeJS.ProcessEnv): ControlStore {
    const host = requireValue(environment, "DATABASE_HOST");
    const port = Number(environment.DATABASE_PORT ?? "5432");
    if (!Number.isInteger(port) || port < 1 || port > 65_535) {
      throw new Error("DATABASE_PORT must be an integer between 1 and 65535");
    }
    return new ControlStore(new Pool({
      host,
      port,
      database: requireValue(environment, "DATABASE_NAME"),
      user: requireValue(environment, "DATABASE_USER"),
      password: requireValue(environment, "DATABASE_PASSWORD"),
      max: 10
    }), capacityValue(environment.CLOUD_MAX_SERVERS), storageValue(environment),
    startQueueValue(environment.CLOUD_START_QUEUE));
  }

  public async initialize(machineToken: string): Promise<Principal> {
    const client = await this.pool.connect();
    try {
      await migrate(client);
      return await bootstrapOperator(client, machineToken);
    } finally {
      client.release();
    }
  }

  public authenticate(token: string): Promise<Principal | null> {
    return authenticateMachineToken(this.pool, token);
  }

  public async assertServerOwnership(principalId: string, serverId: string): Promise<void> {
    const result = await this.pool.query(
      "SELECT 1 FROM servers WHERE id = $1 AND principal_id = $2",
      [serverId, principalId]
    );
    if (result.rowCount !== 1) throw new Error("server_not_found");
  }

  public async claimIdempotencyKey(
    principalId: string,
    clientRequestId: string,
    operation: string,
    requestBody: string,
    database: Pick<pg.PoolClient, "query"> = this.pool
  ): Promise<{ claimed: boolean; record: IdempotencyRecord }> {
    const requestDigest = createHash("sha256").update(requestBody).digest();
    const inserted = await database.query(
      `INSERT INTO idempotency_keys
         (principal_id, client_request_id, operation, request_digest)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT DO NOTHING`,
      [principalId, clientRequestId, operation, requestDigest]
    );
    const result = await database.query<{
      operation: string;
      request_digest: Buffer;
      response_status: number | null;
      response_body: unknown;
    }>(
      `SELECT operation, request_digest, response_status, response_body
         FROM idempotency_keys
        WHERE principal_id = $1 AND client_request_id = $2 FOR UPDATE`,
      [principalId, clientRequestId]
    );
    const row = result.rows[0];
    if (row === undefined) throw new Error("idempotency claim disappeared");
    if (row.operation !== operation || !row.request_digest.equals(requestDigest)) {
      throw new ControlError("idempotency_key_reused", 409);
    }
    return {
      claimed: inserted.rowCount === 1,
      record: {
        operation: row.operation,
        requestDigest: row.request_digest,
        responseStatus: row.response_status,
        responseBody: row.response_body
      }
    };
  }

  public async completeIdempotencyKey(
    principalId: string,
    clientRequestId: string,
    responseStatus: number,
    responseBody: unknown,
    database: Pick<pg.PoolClient, "query"> = this.pool
  ): Promise<void> {
    const result = await database.query(
      `UPDATE idempotency_keys
          SET response_status = $3, response_body = $4, completed_at = now()
        WHERE principal_id = $1
          AND client_request_id = $2
          AND completed_at IS NULL`,
      [principalId, clientRequestId, responseStatus, responseBody]
    );
    if (result.rowCount !== 1) throw new Error("idempotency_key_not_pending");
  }

  public async close(): Promise<void> {
    await this.pool.end();
  }

  public async mutate(principalId: string, requestId: string, mutation: Mutation): Promise<AcceptedMutation> {
    if (!/^[A-Za-z0-9._:-]{1,128}$/.test(requestId)) throw new ControlError("invalid_request_id", 400);
    if (mutation.operation === "create" && !/^[a-z0-9][a-z0-9-]{0,62}$/.test(mutation.name)) {
      throw new ControlError("invalid_server_name", 400);
    }
    const body = mutation.operation === "create" ?
      JSON.stringify({ name: mutation.name, eula_accepted: mutation.eula_accepted }) :
      JSON.stringify({ server_id: mutation.server_id });
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const claim = await this.claimIdempotencyKey(principalId, requestId, mutation.operation, body, client);
      if (claim.record.responseStatus !== null) {
        await client.query("COMMIT");
        return claim.record.responseBody as AcceptedMutation;
      }
      if (this.startQueue) {
        await lockStartQueue(client, principalId);
        await advanceStartQueue(client);
      }
      let server: ServerRecord;
      let startRequest: StartRequestRecord | null = null;
      if (mutation.operation === "create") {
        await assertFreeSpace(this.storage);
        if (this.maxServers > 0) {
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [principalId]);
          const count = await client.query<{ count: string }>(
            "SELECT count(*) AS count FROM servers WHERE principal_id = $1", [principalId]
          );
          if (Number(count.rows[0]?.count ?? "0") >= this.maxServers) {
            throw new ControlError("capacity_unavailable", 409);
          }
        }
        const id = randomUUID();
        const result = await client.query<ServerRecord>(
          `INSERT INTO servers (id, principal_id, logical_name, recipe_id, profile_id, world_identity,
             desired_state, state, generation, last_request_id)
           VALUES ($1, $2, $3, 'paper-26.2-121', 'two-player', $4, 'running', 'queued', 1, $5)
           RETURNING *`, [id, principalId, mutation.name, `cloud-world-${id}`, requestId]
        );
        server = result.rows[0]!;
      } else {
        const current = await client.query<ServerRecord>(
          "SELECT * FROM servers WHERE id = $1 AND principal_id = $2 FOR UPDATE",
          [mutation.server_id, principalId]
        );
        if (current.rows[0] === undefined) throw new ControlError("server_not_found", 404);
        server = current.rows[0];
        startRequest = this.startQueue ? await currentStartRequest(client, server.id) : null;
        if (this.startQueue && mutation.operation === "start" && server.state === "stopping") {
          throw new ControlError("server_stopping", 409);
        }
        if (mutation.operation === "stop" && startRequest !== null &&
            (startRequest.state === "queued" || startRequest.state === "reserved")) {
          await cancelStartRequest(client, startRequest);
          server = (await client.query<ServerRecord>("SELECT * FROM servers WHERE id = $1", [server.id])).rows[0]!;
        } else if (!(mutation.operation === "start" && startRequest !== null &&
                     startRequest.server_generation === server.generation)) {
          const desired = mutation.operation === "start" ? "running" : "stopped";
          const state = desired === "running" ? (server.state === "running" ? "running" : "queued") :
            (server.state === "stopped" ? "stopped" : "stopping");
          const result = await client.query<ServerRecord>(
            `UPDATE servers SET desired_state = $3, state = $4, generation = generation + 1,
               last_request_id = $5, updated_at = now()
             WHERE id = $1 AND principal_id = $2 RETURNING *`,
            [mutation.server_id, principalId, desired, state, requestId]
          );
          server = result.rows[0]!;
        }
      }
      if (this.startQueue && mutation.operation !== "stop" && startRequest === null) {
        startRequest = await recordStartRequest(client, server, requestId);
        if (startRequest.state === "admitted" && server.state !== "running") {
          server = (await client.query<ServerRecord>(
            "UPDATE servers SET state = 'provisioning' WHERE id = $1 RETURNING *", [server.id]
          )).rows[0]!;
        }
      }
      if (server.desired_state === "running" && (!this.startQueue || startRequest?.state === "admitted")) {
        await client.query(
          `INSERT INTO runs (id, server_id, state) VALUES ($1, $2, 'queued')
           ON CONFLICT (server_id) WHERE state IN ('queued', 'provisioning', 'running', 'stopping') DO NOTHING`,
          [randomUUID(), server.id]
        );
      }
      const response: AcceptedMutation = { request_id: requestId, server_id: server.id, state: server.state,
        status_url: `/v1/servers/${server.id}`,
        ...(mutation.operation !== "stop" && startRequest !== null ?
          { start_request_id: startRequest.id, queue_url: `/v1/start-requests/${startRequest.id}` } : {}) };
      if (this.startQueue && mutation.operation === "stop") await advanceStartQueue(client);
      await client.query(
        `INSERT INTO events (principal_id, server_id, event_type, payload)
         VALUES ($1, $2, 'intent_recorded', $3)`,
        [principalId, server.id, {
          request_id: requestId,
          operation: mutation.operation,
          generation: server.generation,
          ...(mutation.operation === "create" ? { eula_accepted: true } : {})
        }]
      );
      await this.completeIdempotencyKey(principalId, requestId, 202, response, client);
      await client.query("COMMIT");
      return response;
    } catch (error) {
      await client.query("ROLLBACK");
      if (error instanceof pg.DatabaseError && error.constraint === "servers_principal_id_logical_name_key") {
        throw new ControlError("server_name_taken", 409);
      }
      throw error;
    } finally {
      client.release();
    }
  }

  public async getServer(principalId: string, serverId: string): Promise<ServerRecord> {
    const result = await this.pool.query<ServerRecord>(
      "SELECT * FROM servers WHERE id = $1 AND principal_id = $2", [serverId, principalId]
    );
    if (result.rows[0] === undefined) throw new ControlError("server_not_found", 404);
    return result.rows[0];
  }

  public async getStartRequest(principalId: string, requestId: string): Promise<StartRequestStatus> {
    if (!this.startQueue) throw new ControlError("start_request_not_found", 404);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await lockStartQueue(client, principalId);
      await advanceStartQueue(client);
      const request = await readStartRequest(client, principalId, requestId);
      await client.query("COMMIT");
      return request;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

  public async cancelStartRequest(principalId: string, requestId: string, clientRequestId: string): Promise<{
    start_request_id: string; server_id: string; state: "cancelled"; queue_url: string;
  }> {
    if (!this.startQueue) throw new ControlError("start_request_not_found", 404);
    if (!/^[A-Za-z0-9._:-]{1,128}$/.test(clientRequestId)) throw new ControlError("invalid_request_id", 400);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const claim = await this.claimIdempotencyKey(principalId, clientRequestId, "cancel_start_request",
        JSON.stringify({ start_request_id: requestId }), client);
      if (claim.record.responseStatus !== null) {
        await client.query("COMMIT");
        return claim.record.responseBody as Awaited<ReturnType<ControlStore["cancelStartRequest"]>>;
      }
      await lockStartQueue(client, principalId);
      await advanceStartQueue(client);
      const request = await readStartRequest(client, principalId, requestId);
      await cancelStartRequest(client, request);
      await advanceStartQueue(client);
      const response = { start_request_id: request.id, server_id: request.server_id,
        state: "cancelled" as const, queue_url: `/v1/start-requests/${request.id}` };
      await this.completeIdempotencyKey(principalId, clientRequestId, 202, response, client);
      await client.query("COMMIT");
      return response;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

  public async advanceStartQueue(): Promise<void> {
    if (!this.startQueue) return;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('cloud:start-queue', 0))");
      await advanceStartQueue(client);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

  public async shouldReconcile(server: ServerRecord, client: pg.PoolClient): Promise<boolean> {
    if (!this.startQueue) return true;
    const current = await this.getDesiredServer(server.id, client);
    if (current === null || current.generation !== server.generation) return false;
    const run = await client.query("SELECT 1 FROM runs WHERE server_id = $1 AND finished_at IS NULL", [server.id]);
    if (server.desired_state === "stopped") return run.rowCount === 1;
    const request = await currentStartRequest(client, server.id);
    return request === null ? run.rowCount === 1 :
      request.state === "admitted" && request.server_generation === server.generation;
  }

  public async isStartAdmitted(server: ServerRecord, client: pg.PoolClient): Promise<boolean> {
    if (!this.startQueue) return true;
    const request = await currentStartRequest(client, server.id);
    return request?.state === "admitted" && request.server_generation === server.generation;
  }

  public async listServerIds(): Promise<string[]> {
    const result = await this.pool.query<{ id: string }>(
      "SELECT id FROM servers WHERE last_request_id IS NOT NULL ORDER BY updated_at"
    );
    return result.rows.map(row => row.id);
  }

  public async getDesiredServer(serverId: string, client: pg.PoolClient): Promise<ServerRecord | null> {
    const result = await client.query<ServerRecord>("SELECT * FROM servers WHERE id = $1", [serverId]);
    return result.rows[0] ?? null;
  }

  public async withServerLock(serverId: string, work: (client: pg.PoolClient) => Promise<void>): Promise<void> {
    const client = await this.pool.connect();
    let locked = false;
    try {
      const result = await client.query<{ locked: boolean }>(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked", [serverId]
      );
      locked = result.rows[0]!.locked;
      if (locked) await work(client);
    } finally {
      try {
        if (locked) await client.query("SELECT pg_advisory_unlock(hashtextextended($1, 0))", [serverId]);
      } finally {
        client.release();
      }
    }
  }

  public async recordObservation(server: ServerRecord, observed: RuntimeObservation, client: pg.PoolClient): Promise<void> {
    const state: ServerState = server.desired_state === "running" ?
      (observed.state === "ready" ? "running" : "provisioning") :
      (observed.state === "absent" || observed.state === "stopped" ? "stopped" : "stopping");
    await client.query("BEGIN");
    try {
      if (this.startQueue) await lockStartQueue(client, server.principal_id);
      const updated = await client.query(
        `UPDATE servers SET state = $3, observed_generation = $2,
           endpoint = COALESCE($4::jsonb, endpoint), updated_at = now()
         WHERE id = $1 AND generation = $2 AND
           (state IS DISTINCT FROM $3 OR observed_generation <> $2 OR
            ($4::jsonb IS NOT NULL AND endpoint IS DISTINCT FROM $4::jsonb))`,
        [server.id, server.generation, state, observed.endpoint ?? null]
      );
      if (updated.rowCount === 1) {
        const run = await client.query<{ id: string }>(
          `UPDATE runs SET state = $2, updated_at = now(),
             finished_at = CASE WHEN $2 = 'stopped' THEN now() ELSE NULL END
           WHERE server_id = $1 AND finished_at IS NULL RETURNING id`, [server.id, state]
        );
        await client.query(
          `INSERT INTO events (principal_id, server_id, run_id, event_type, payload)
           VALUES ($1, $2, $3, 'state_observed', $4)`,
          [server.principal_id, server.id, run.rows[0]?.id ?? null,
            { request_id: server.last_request_id, generation: server.generation, state }]
        );
        if (this.startQueue && state === "stopped") {
          await client.query(
            `UPDATE start_requests SET state = 'finished', terminal_at = clock_timestamp()
             WHERE server_id = $1 AND state = 'admitted'`, [server.id]
          );
          await advanceStartQueue(client);
        }
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  }
}

function requireValue(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name];
  if (value === undefined || value.length === 0) throw new Error(`${name} is required`);
  return value;
}

function capacityValue(value: string | undefined): number {
  if (value === undefined) return 0;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) throw new Error("CLOUD_MAX_SERVERS must be a non-negative integer");
  return parsed;
}

function startQueueValue(value: string | undefined): boolean {
  if (value === undefined) return false;
  if (value === "beta") return true;
  throw new Error("CLOUD_START_QUEUE must be beta when set");
}

function storageValue(environment: NodeJS.ProcessEnv): StorageReserve | null {
  const path = environment.CLOUD_STORAGE_PATH;
  if (path === undefined || path.length === 0) return null;
  const mib = Number(environment.CLOUD_STORAGE_MIN_FREE_MIB ?? "2048");
  if (!Number.isInteger(mib) || mib < 0) throw new Error("CLOUD_STORAGE_MIN_FREE_MIB must be a non-negative integer");
  return { path, minFreeBytes: mib * 1024 * 1024 };
}
