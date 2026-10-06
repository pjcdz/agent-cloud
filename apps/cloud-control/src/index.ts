import { createServer } from "node:http";

import { createRequestHandler } from "./app.js";
import { ControlStore } from "./database.js";
import { DockerRuntime } from "./docker.js";
import { KubernetesRuntime } from "./kubernetes.js";
import { Reconciler } from "./lifecycle.js";

const host = process.env.HOST ?? "127.0.0.1";
const port = Number(process.env.PORT ?? "3000");

if (!Number.isInteger(port) || port < 1 || port > 65_535) {
  throw new Error("PORT must be an integer between 1 and 65535");
}

const machineToken = process.env.CLOUD_MACHINE_TOKEN;
if (machineToken === undefined) throw new Error("CLOUD_MACHINE_TOKEN is required");

const store = ControlStore.fromEnvironment(process.env);
await store.initialize(machineToken);

const server = createServer(createRequestHandler(store));
const runtime = process.env.CLOUD_RUNTIME === "kubernetes" ? KubernetesRuntime.inCluster() :
  process.env.CLOUD_RUNTIME === "docker" ? DockerRuntime.fromEnvironment(process.env) : null;
const reconciler = runtime === null ? null : new Reconciler(store, runtime);
let reconciling = false;
let pending = Promise.resolve();
const timer = setInterval(() => {
  if (reconciling) return;
  reconciling = true;
  pending = store.advanceStartQueue().then(() => reconciler?.tick()).catch(error => {
    process.stderr.write(`${JSON.stringify({ event: "reconciliation_failed", error: String(error) })}\n`);
  }).finally(() => { reconciling = false; });
}, 1000);

async function shutdown(): Promise<void> {
  clearInterval(timer);
  await new Promise<void>(resolve => server.close(() => resolve()));
  await pending;
  await store.close();
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());

server.listen(port, host, () => {
  process.stdout.write(
    `${JSON.stringify({ event: "listening", host, port, service: "cloud-control" })}\n`
  );
});
