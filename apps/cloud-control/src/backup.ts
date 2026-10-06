import pg from "pg";

import { ControlError } from "./lifecycle.js";

export type BackupState = "pending" | "uploaded" | "failed" | "restore_tested";

export interface BackupArtifact {
  readonly sha256: string;
  readonly sizeBytes: number;
}

export interface BackupRecord {
  readonly server_id: string;
  readonly generation: number;
  readonly request_id: string;
  readonly object_key: string;
  readonly sha256: string;
  readonly size_bytes: string;
  readonly state: BackupState;
  readonly created_at: Date;
  readonly updated_at: Date;
  readonly uploaded_at: Date | null;
  readonly failed_at: Date | null;
  readonly restore_tested_at: Date | null;
  readonly failure_reason: string | null;
}

export class BackupStore {
  public constructor(private readonly pool: pg.Pool) {}

  public async createGeneration(
    serverId: string, requestId: string, artifact: BackupArtifact
  ): Promise<BackupRecord> {
    if (typeof requestId !== "string" || requestId.length === 0 || requestId.length > 128 || /[^A-Za-z0-9._:-]/.test(requestId)) {
      throw new ControlError("invalid_request_id", 400);
    }
    if (typeof artifact.sha256 !== "string" || artifact.sha256.length !== 64 || /[^a-fA-F0-9]/.test(artifact.sha256)) {
      throw new ControlError("invalid_backup_digest", 400);
    }
    if (!Number.isSafeInteger(artifact.sizeBytes) || artifact.sizeBytes < 0) {
      throw new ControlError("invalid_backup_size", 400);
    }
    const sha256 = artifact.sha256.toLowerCase();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO backup_generation_counters (server_id)
         SELECT id FROM servers WHERE id = $1 ON CONFLICT DO NOTHING`, [serverId]
      );
      const counter = await client.query<{ last_generation: number }>(
        "SELECT last_generation FROM backup_generation_counters WHERE server_id = $1 FOR UPDATE", [serverId]
      );
      if (counter.rows[0] === undefined) throw new ControlError("server_not_found", 404);
      const replay = await client.query<BackupRecord>(
        "SELECT * FROM backups WHERE server_id = $1 AND request_id = $2", [serverId, requestId]
      );
      let backup = replay.rows[0];
      if (backup !== undefined) {
        if (backup.sha256 !== sha256 || backup.size_bytes !== String(artifact.sizeBytes)) {
          throw new ControlError("backup_request_reused", 409);
        }
      } else {
        if (counter.rows[0].last_generation === 2_147_483_647) {
          throw new ControlError("backup_generation_exhausted", 409);
        }
        const generation = counter.rows[0].last_generation + 1;
        await client.query(
          "UPDATE backup_generation_counters SET last_generation = $2 WHERE server_id = $1", [serverId, generation]
        );
        const inserted = await client.query<BackupRecord>(
          `INSERT INTO backups (server_id, generation, request_id, sha256, size_bytes)
           VALUES ($1, $2, $3, $4, $5) RETURNING *`, [serverId, generation, requestId, sha256, artifact.sizeBytes]
        );
        backup = inserted.rows[0]!;
      }
      await client.query("COMMIT");
      return backup;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  public async getGeneration(serverId: string, generation: number): Promise<BackupRecord | null> {
    validateGeneration(generation);
    const result = await this.pool.query<BackupRecord>(
      "SELECT * FROM backups WHERE server_id = $1 AND generation = $2", [serverId, generation]
    );
    return result.rows[0] ?? null;
  }

  public async getLatestGood(serverId: string): Promise<BackupRecord | null> {
    const result = await this.pool.query<BackupRecord>(
      `SELECT * FROM backups WHERE server_id = $1 AND state IN ('uploaded', 'restore_tested')
       ORDER BY generation DESC LIMIT 1`, [serverId]
    );
    return result.rows[0] ?? null;
  }

  public markUploaded(serverId: string, generation: number): Promise<BackupRecord> {
    return this.transition(serverId, generation, "uploaded");
  }

  public async markFailed(serverId: string, generation: number, reason: string): Promise<BackupRecord> {
    if (typeof reason !== "string" || reason.trim().length === 0 || reason.length > 1024) {
      throw new ControlError("invalid_backup_failure_reason", 400);
    }
    return this.transition(serverId, generation, "failed", reason);
  }

  public retryGeneration(serverId: string, generation: number): Promise<BackupRecord> {
    return this.transition(serverId, generation, "pending");
  }

  public markRestoreTested(serverId: string, generation: number): Promise<BackupRecord> {
    return this.transition(serverId, generation, "restore_tested");
  }

  private async transition(
    serverId: string, generation: number, state: BackupState, failureReason: string | null = null
  ): Promise<BackupRecord> {
    validateGeneration(generation);
    try {
      const result = await this.pool.query<BackupRecord>(
        `UPDATE backups SET state = $3, failure_reason = $4
         WHERE server_id = $1 AND generation = $2 RETURNING *`, [serverId, generation, state, failureReason]
      );
      if (result.rows[0] === undefined) throw new ControlError("backup_not_found", 404);
      return result.rows[0];
    } catch (error) {
      if (error instanceof pg.DatabaseError && error.code === "23514") {
        throw new ControlError("invalid_backup_transition", 409);
      }
      throw error;
    }
  }
}

function validateGeneration(generation: number): void {
  if (!Number.isInteger(generation) || generation < 1 || generation > 2_147_483_647) {
    throw new ControlError("invalid_backup_generation", 400);
  }
}
