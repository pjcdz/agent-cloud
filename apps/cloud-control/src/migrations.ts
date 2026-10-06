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
    version: 4,
    name: "backup_generations_and_states",
    sql: `
CREATE TABLE backup_generation_counters (
  server_id uuid PRIMARY KEY REFERENCES servers(id),
  last_generation integer NOT NULL DEFAULT 0 CHECK (last_generation >= 0)
);

CREATE TABLE backups (
  server_id uuid NOT NULL REFERENCES servers(id),
  generation integer NOT NULL CHECK (generation > 0),
  request_id text NOT NULL CHECK (request_id ~ '^[A-Za-z0-9._:-]{1,128}$'),
  object_key text GENERATED ALWAYS AS
    ('servers/' || server_id::text || '/backups/' || generation::text || '/world') STORED UNIQUE,
  sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
  size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 9007199254740991),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'uploaded', 'failed', 'restore_tested')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  uploaded_at timestamptz,
  failed_at timestamptz,
  restore_tested_at timestamptz,
  failure_reason text CHECK (length(btrim(failure_reason)) > 0 AND length(failure_reason) <= 1024),
  PRIMARY KEY (server_id, generation),
  UNIQUE (server_id, request_id),
  CHECK ((state IN ('uploaded', 'restore_tested')) = (uploaded_at IS NOT NULL)),
  CHECK ((state = 'failed') = (failed_at IS NOT NULL)),
  CHECK ((state = 'failed') = (failure_reason IS NOT NULL)),
  CHECK ((state = 'restore_tested') = (restore_tested_at IS NOT NULL)),
  CHECK (updated_at >= created_at),
  CHECK (uploaded_at >= created_at),
  CHECK (failed_at >= created_at),
  CHECK (restore_tested_at >= uploaded_at)
);

CREATE INDEX backups_latest_good ON backups (server_id, generation DESC)
  WHERE state IN ('uploaded', 'restore_tested');

CREATE FUNCTION guard_backup_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.state <> 'pending' THEN
      RAISE EXCEPTION 'backup must begin pending' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;

  IF ROW(NEW.server_id, NEW.generation, NEW.request_id, NEW.sha256, NEW.size_bytes,
         NEW.created_at, NEW.uploaded_at, NEW.failed_at, NEW.restore_tested_at, NEW.updated_at)
     IS DISTINCT FROM
     ROW(OLD.server_id, OLD.generation, OLD.request_id, OLD.sha256, OLD.size_bytes,
         OLD.created_at, OLD.uploaded_at, OLD.failed_at, OLD.restore_tested_at, OLD.updated_at) THEN
    RAISE EXCEPTION 'backup metadata is immutable' USING ERRCODE = '23514';
  END IF;

  IF NEW.state = OLD.state THEN
    IF NEW.failure_reason IS DISTINCT FROM OLD.failure_reason THEN
      RAISE EXCEPTION 'backup result cannot change on replay' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
  END IF;

  IF NOT ((OLD.state = 'pending' AND NEW.state IN ('uploaded', 'failed')) OR
          (OLD.state = 'failed' AND NEW.state = 'pending') OR
          (OLD.state = 'uploaded' AND NEW.state = 'restore_tested')) THEN
    RAISE EXCEPTION 'invalid backup transition from % to %', OLD.state, NEW.state USING ERRCODE = '23514';
  END IF;

  NEW.updated_at := clock_timestamp();
  IF NEW.state = 'uploaded' THEN NEW.uploaded_at := NEW.updated_at; END IF;
  NEW.failed_at := CASE WHEN NEW.state = 'failed' THEN NEW.updated_at ELSE NULL END;
  IF NEW.state = 'restore_tested' THEN NEW.restore_tested_at := NEW.updated_at; END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER backups_metadata_guard BEFORE INSERT OR UPDATE ON backups
  FOR EACH ROW EXECUTE FUNCTION guard_backup_metadata();
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
