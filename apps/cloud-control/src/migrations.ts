import type { PoolClient } from "pg";

export interface Migration {
  readonly version: number;
  readonly name: string;
  readonly sql: string;
}

export const migrations: readonly Migration[] = [
  {
    version: 1,
    name: "minimum_control_plane",
    sql: `
CREATE TABLE principals (
  id uuid PRIMARY KEY,
  subject text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE machine_tokens (
  id uuid PRIMARY KEY,
  principal_id uuid NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
  token_digest bytea NOT NULL UNIQUE,
  label text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  CHECK (octet_length(token_digest) = 32)
);

CREATE TABLE servers (
  id uuid PRIMARY KEY,
  principal_id uuid NOT NULL REFERENCES principals(id),
  logical_name text NOT NULL,
  recipe_id text NOT NULL,
  profile_id text NOT NULL,
  world_identity text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (principal_id, logical_name),
  UNIQUE (id, principal_id)
);

CREATE TABLE runs (
  id uuid PRIMARY KEY,
  server_id uuid NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
  state text NOT NULL CHECK (state IN (
    'queued', 'provisioning', 'running', 'stopping', 'stopped', 'failed'
  )),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  CHECK ((state IN ('stopped', 'failed')) = (finished_at IS NOT NULL))
);

CREATE UNIQUE INDEX runs_one_active_per_server
  ON runs (server_id)
  WHERE state IN ('queued', 'provisioning', 'running', 'stopping');

CREATE TABLE idempotency_keys (
  principal_id uuid NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
  client_request_id text NOT NULL,
  operation text NOT NULL,
  request_digest bytea NOT NULL,
  response_status integer,
  response_body jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  PRIMARY KEY (principal_id, client_request_id),
  CHECK (octet_length(request_digest) = 32),
  CHECK ((response_status IS NULL) = (completed_at IS NULL)),
  CHECK (response_status IS NULL OR response_status BETWEEN 100 AND 599)
);

CREATE TABLE events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  principal_id uuid NOT NULL REFERENCES principals(id),
  server_id uuid REFERENCES servers(id) ON DELETE CASCADE,
  run_id uuid REFERENCES runs(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (server_id, principal_id) REFERENCES servers(id, principal_id)
);

CREATE INDEX events_server_order ON events (server_id, id);
CREATE INDEX events_run_order ON events (run_id, id);
`
  },
  {
    version: 2,
    name: "event_run_ownership",
    sql: `
ALTER TABLE runs ADD CONSTRAINT runs_id_server_unique UNIQUE (id, server_id);
ALTER TABLE events ADD CONSTRAINT events_run_server_fk
  FOREIGN KEY (run_id, server_id) REFERENCES runs(id, server_id);
ALTER TABLE events ADD CONSTRAINT events_run_requires_server
  CHECK (run_id IS NULL OR server_id IS NOT NULL);
`
  },
  {
    version: 3,
    name: "durable_desired_state",
    sql: `
ALTER TABLE servers
  ADD COLUMN desired_state text NOT NULL DEFAULT 'stopped'
    CHECK (desired_state IN ('running', 'stopped')),
  ADD COLUMN state text NOT NULL DEFAULT 'stopped'
    CHECK (state IN ('queued', 'provisioning', 'running', 'stopping', 'stopped')),
  ADD COLUMN generation integer NOT NULL DEFAULT 0 CHECK (generation >= 0),
  ADD COLUMN observed_generation integer NOT NULL DEFAULT 0
    CHECK (observed_generation >= 0 AND observed_generation <= generation),
  ADD COLUMN last_request_id text,
  ADD COLUMN endpoint jsonb,
  ADD FOREIGN KEY (principal_id, last_request_id)
    REFERENCES idempotency_keys (principal_id, client_request_id);
`
  },
  {
    version: 5,
    name: "durable_start_queue",
    sql: `
CREATE TABLE start_requests (
  id uuid PRIMARY KEY,
  principal_id uuid NOT NULL,
  server_id uuid NOT NULL,
  client_request_id text NOT NULL,
  server_generation integer NOT NULL CHECK (server_generation > 0),
  arrival_order bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  arrived_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  state text NOT NULL CHECK (state IN ('queued', 'reserved', 'admitted', 'cancelled', 'expired', 'finished')),
  reserved_at timestamptz,
  expires_at timestamptz,
  admitted_at timestamptz,
  terminal_at timestamptz,
  FOREIGN KEY (server_id, principal_id) REFERENCES servers(id, principal_id),
  FOREIGN KEY (principal_id, client_request_id) REFERENCES idempotency_keys(principal_id, client_request_id),
  CHECK ((reserved_at IS NULL) = (expires_at IS NULL)),
  CHECK (expires_at > reserved_at),
  CHECK (state <> 'queued' OR (reserved_at IS NULL AND admitted_at IS NULL)),
  CHECK (state <> 'reserved' OR (reserved_at IS NOT NULL AND admitted_at IS NULL)),
  CHECK ((state IN ('admitted', 'finished')) = (admitted_at IS NOT NULL)),
  CHECK (state <> 'expired' OR reserved_at IS NOT NULL),
  CHECK ((state IN ('cancelled', 'expired', 'finished')) = (terminal_at IS NOT NULL))
);

CREATE UNIQUE INDEX start_requests_one_current_per_server ON start_requests(server_id)
  WHERE state IN ('queued', 'reserved', 'admitted');
CREATE UNIQUE INDEX start_requests_one_qualified_slot ON start_requests((true))
  WHERE state IN ('reserved', 'admitted');
`
  }
];

export async function migrate(client: PoolClient): Promise<void> {
  await client.query("BEGIN");
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version integer PRIMARY KEY,
        name text NOT NULL UNIQUE,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);

    await client.query("LOCK TABLE schema_migrations IN EXCLUSIVE MODE");

    for (const migration of migrations) {
      const result = await client.query<{ version: number }>(
        "SELECT version FROM schema_migrations WHERE version = $1",
        [migration.version]
      );
      if (result.rowCount === 0) {
        await client.query(migration.sql);
        await client.query(
          "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
          [migration.version, migration.name]
        );
      }
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
