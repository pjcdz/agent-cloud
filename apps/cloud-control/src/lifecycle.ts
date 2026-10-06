import type { ControlStore } from "./database.js";

export type ServerState = "queued" | "provisioning" | "running" | "stopping" | "stopped";
export interface Endpoint { host: string; port: number }
export interface ServerRecord {
  id: string;
  principal_id: string;
  logical_name: string;
  recipe_id: string;
  profile_id: string;
  world_identity: string;
  desired_state: "running" | "stopped";
  state: ServerState;
  generation: number;
  observed_generation: number;
  last_request_id: string | null;
  endpoint: Endpoint | null;
}
export type Mutation = { operation: "create"; name: string; eula_accepted: true } |
  { operation: "start" | "stop"; server_id: string };
export interface AcceptedMutation {
  request_id: string;
  server_id: string;
  state: ServerState;
  status_url: string;
  start_request_id?: string;
  queue_url?: string;
}
export interface RuntimeObservation {
  state: "absent" | "stopped" | "starting" | "ready" | "stopping";
  endpoint?: Endpoint;
}
export interface Runtime {
  observe(server: ServerRecord): Promise<RuntimeObservation>;
  create(server: ServerRecord): Promise<void>;
  start(server: ServerRecord): Promise<void>;
  stop(server: ServerRecord): Promise<void>;
}

export class ControlError extends Error {
  public constructor(public readonly code: string, public readonly status: number) {
    super(code);
  }
}

export class Reconciler {
  public constructor(private readonly store: ControlStore, private readonly runtime: Runtime) {}

  public async reconcile(serverId: string): Promise<void> {
    await this.store.withServerLock(serverId, async (client) => {
      const server = await this.store.getDesiredServer(serverId, client);
      if (server === null) return;
      if (!await this.store.shouldReconcile(server, client)) return;
      const observed = await this.runtime.observe(server);
      await this.store.recordObservation(server, observed, client);
      if (!await this.store.shouldReconcile(server, client)) return;
      if (server.desired_state === "running" && await this.store.isStartAdmitted(server, client)) {
        if (observed.state === "absent") await this.runtime.create(server);
        else if (observed.state === "stopped") await this.runtime.start(server);
      } else if (server.desired_state === "stopped" && (observed.state === "ready" || observed.state === "starting")) {
        await this.runtime.stop(server);
      }
    });
  }

  public async tick(): Promise<void> {
    const errors: unknown[] = [];
    for (const id of await this.store.listServerIds()) {
      try { await this.reconcile(id); }
      catch (error) { errors.push(error); }
    }
    if (errors.length > 0) throw new AggregateError(errors, "reconciliation_incomplete");
  }
}
