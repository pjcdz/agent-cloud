import { randomUUID } from "node:crypto";
import type { PoolClient } from "pg";

import { ControlError, type ServerRecord } from "./lifecycle.js";

export type StartRequestState = "queued" | "reserved" | "admitted" | "cancelled" | "expired" | "finished";
export interface StartRequestRecord {
  id: string;
  principal_id: string;
  server_id: string;
  client_request_id: string;
  server_generation: number;
  arrival_order: string;
  arrived_at: Date;
  state: StartRequestState;
  reserved_at: Date | null;
  expires_at: Date | null;
  admitted_at: Date | null;
  terminal_at: Date | null;
}
export interface StartRequestStatus extends StartRequestRecord { valid_requests_ahead: number }

export async function lockStartQueue(client: PoolClient, principalId: string): Promise<void> {
  await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [principalId]);
  await client.query("SELECT pg_advisory_xact_lock(hashtextextended('cloud:start-queue', 0))");
}

export async function stopPendingIntent(client: PoolClient, request: StartRequestRecord): Promise<void> {
  await client.query(
    `UPDATE servers SET desired_state = 'stopped', state = 'stopped',
       generation = generation + 1, updated_at = clock_timestamp()
     WHERE id = $1 AND generation = $2 AND NOT EXISTS (
       SELECT 1 FROM runs WHERE server_id = $1 AND finished_at IS NULL
     )`, [request.server_id, request.server_generation]
  );
}

export async function advanceStartQueue(client: PoolClient): Promise<void> {
  const expired = await client.query<StartRequestRecord>(
    `UPDATE start_requests SET state = 'expired', terminal_at = clock_timestamp()
     WHERE state = 'reserved' AND expires_at <= clock_timestamp() RETURNING *`
  );
  for (const request of expired.rows) await stopPendingIntent(client, request);
  const occupied = await client.query<{ occupied: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM runs WHERE finished_at IS NULL) OR
       EXISTS (SELECT 1 FROM start_requests WHERE state IN ('reserved', 'admitted')) AS occupied`
  );
  if (occupied.rows[0]!.occupied) return;
  await client.query(
    `UPDATE start_requests SET state = 'reserved', reserved_at = statement_timestamp(),
       expires_at = statement_timestamp() + interval '5 minutes'
     WHERE id = (SELECT id FROM start_requests WHERE state = 'queued'
       ORDER BY arrival_order LIMIT 1)`
  );
}

export async function currentStartRequest(client: PoolClient, serverId: string): Promise<StartRequestRecord | null> {
  const result = await client.query<StartRequestRecord>(
    "SELECT * FROM start_requests WHERE server_id = $1 AND state IN ('queued', 'reserved', 'admitted')",
    [serverId]
  );
  return result.rows[0] ?? null;
}

export async function recordStartRequest(
  client: PoolClient, server: ServerRecord, requestId: string
): Promise<StartRequestRecord> {
  if (server.recipe_id !== "paper-26.2-121" || server.profile_id !== "two-player") {
    throw new ControlError("profile_not_qualified", 409);
  }
  const occupied = await client.query<{ occupied: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM runs WHERE finished_at IS NULL AND server_id <> $1) OR
       EXISTS (SELECT 1 FROM start_requests WHERE state IN ('reserved', 'admitted')) AS occupied`, [server.id]
  );
  const state = occupied.rows[0]!.occupied ? "queued" : "admitted";
  const result = await client.query<StartRequestRecord>(
    `INSERT INTO start_requests (id, principal_id, server_id, client_request_id, server_generation,
       state, admitted_at) VALUES ($1, $2, $3, $4, $5, $6,
       CASE WHEN $6 = 'admitted' THEN clock_timestamp() ELSE NULL END) RETURNING *`,
    [randomUUID(), server.principal_id, server.id, requestId, server.generation, state]
  );
  return result.rows[0]!;
}

export async function cancelStartRequest(client: PoolClient, request: StartRequestRecord): Promise<void> {
  if (request.state === "cancelled") return;
  if (request.state !== "queued" && request.state !== "reserved") {
    throw new ControlError("start_request_not_pending", 409);
  }
  await client.query(
    "UPDATE start_requests SET state = 'cancelled', terminal_at = clock_timestamp() WHERE id = $1", [request.id]
  );
  await stopPendingIntent(client, request);
}

export async function readStartRequest(
  client: PoolClient, principalId: string, requestId: string
): Promise<StartRequestStatus> {
  const result = await client.query<StartRequestStatus>(
    `SELECT request.*, (SELECT count(*)::int FROM start_requests ahead
       WHERE ahead.arrival_order < request.arrival_order AND
         (ahead.state = 'queued' OR (ahead.state = 'reserved' AND ahead.expires_at > clock_timestamp()))
       AND request.state IN ('queued', 'reserved')) AS valid_requests_ahead
     FROM start_requests request WHERE request.id = $1 AND request.principal_id = $2`, [requestId, principalId]
  );
  if (result.rows[0] === undefined) throw new ControlError("start_request_not_found", 404);
  return result.rows[0];
}
