import { createHash, randomUUID } from "node:crypto";
import { statfs } from "node:fs/promises";

import pg from "pg";

import { authenticateMachineToken, bootstrapOperator, type Principal } from "./auth.js";
import { migrate } from "./migrations.js";
import { ControlError, type AcceptedMutation, type Mutation, type RuntimeObservation, type ServerRecord, type ServerState } from "./lifecycle.js";

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
    private readonly betaAccountQuota = false
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
    accountQuotaValue(environment.CLOUD_ACCOUNT_QUOTA));
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
      if (this.betaAccountQuota && mutation.operation !== "stop") {
        await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [principalId]);
      }
      let server: ServerRecord;
      if (mutation.operation === "create") {
        if (this.betaAccountQuota) await this.assertAccountQuota(principalId, null, client);
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
        if (this.betaAccountQuota && mutation.operation === "start") {
          await this.assertAccountQuota(principalId, mutation.server_id, client);
        }
        const desired = mutation.operation === "start" ? "running" : "stopped";
        const state = desired === "running" ? (current.rows[0].state === "running" ? "running" : "queued") :
          (current.rows[0].state === "stopped" ? "stopped" : "stopping");
        const result = await client.query<ServerRecord>(
          `UPDATE servers SET desired_state = $3, state = $4, generation = generation + 1,
             last_request_id = $5, updated_at = now()
           WHERE id = $1 AND principal_id = $2 RETURNING *`,
          [mutation.server_id, principalId, desired, state, requestId]
        );
        server = result.rows[0]!;
      }
      if (server.desired_state === "running") {
        await client.query(
          `INSERT INTO runs (id, server_id, state) VALUES ($1, $2, 'queued')
           ON CONFLICT (server_id) WHERE state IN ('queued', 'provisioning', 'running', 'stopping') DO NOTHING`,
          [randomUUID(), server.id]
        );
      }
      const response: AcceptedMutation = { request_id: requestId, server_id: server.id, state: server.state,
        status_url: `/v1/servers/${server.id}` };
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

  private async assertAccountQuota(principalId: string, serverId: string | null, client: pg.PoolClient): Promise<void> {
    const counts = await client.query<{ logical: number; active: number }>(
      `SELECT count(*)::int AS logical,
              count(*) FILTER (WHERE id IS DISTINCT FROM $2::uuid AND
                (desired_state = 'running' OR EXISTS (
                  SELECT 1 FROM runs WHERE server_id = servers.id AND finished_at IS NULL
                )))::int AS active
         FROM servers WHERE principal_id = $1`,
      [principalId, serverId]
    );
    const { logical, active } = counts.rows[0]!;
    if ((serverId === null && logical >= 1) || active >= 1) {
      throw new ControlError("quota_exceeded", 409);
    }
  }

  public async getServer(principalId: string, serverId: string): Promise<ServerRecord> {
    const result = await this.pool.query<ServerRecord>(
      "SELECT * FROM servers WHERE id = $1 AND principal_id = $2", [serverId, principalId]
    );
    if (result.rows[0] === undefined) throw new ControlError("server_not_found", 404);
    return result.rows[0];
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

function accountQuotaValue(value: string | undefined): boolean {
  if (value === undefined) return false;
  if (value === "beta") return true;
  throw new Error("CLOUD_ACCOUNT_QUOTA must be beta when set");
}

function storageValue(environment: NodeJS.ProcessEnv): StorageReserve | null {
  const path = environment.CLOUD_STORAGE_PATH;
  if (path === undefined || path.length === 0) return null;
  const mib = Number(environment.CLOUD_STORAGE_MIN_FREE_MIB ?? "2048");
  if (!Number.isInteger(mib) || mib < 0) throw new Error("CLOUD_STORAGE_MIN_FREE_MIB must be a non-negative integer");
  return { path, minFreeBytes: mib * 1024 * 1024 };
}
