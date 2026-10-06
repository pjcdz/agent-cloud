import type { IncomingMessage, ServerResponse } from "node:http";

import { parseBearerToken, type Principal } from "./auth.js";
import type { ControlStore } from "./database.js";
import { ControlError, type ServerRecord } from "./lifecycle.js";
import type { StartRequestStatus } from "./start-requests.js";

type ControlApi = Pick<ControlStore, "authenticate" | "mutate" | "getServer"> &
  Partial<Pick<ControlStore, "getStartRequest" | "cancelStartRequest">>;

const MAX_JSON_BYTES = 16 * 1024;
const UUID = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;

function sendJson(
  response: ServerResponse,
  statusCode: number,
  body: unknown
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8"
  });
  response.end(JSON.stringify(body));
}

async function requirePrincipal(
  request: IncomingMessage,
  response: ServerResponse,
  authenticator: Pick<ControlApi, "authenticate">
): Promise<Principal | null> {
  const token = parseBearerToken(request.headers.authorization);
  const principal = token === null ? null : await authenticator.authenticate(token);
  if (principal === null) {
    response.setHeader("www-authenticate", "Bearer");
    sendJson(response, 401, { error: "unauthorized" });
  }
  return principal;
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const contentType = request.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") throw new ControlError("unsupported_media_type", 415);
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_JSON_BYTES) throw new ControlError("request_too_large", 413);
    chunks.push(buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new ControlError("invalid_json", 400);
  }
}

function parseCreateRequest(body: unknown): { client_request_id: string; name: string } {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new ControlError("invalid_request", 400);
  }
  const input = body as Record<string, unknown>;
  if (typeof input.client_request_id !== "string" || typeof input.name !== "string") {
    throw new ControlError("invalid_request", 400);
  }
  if (input.eula_accepted === undefined || input.eula_accepted === false) {
    throw new ControlError("action_required", 409);
  }
  if (input.eula_accepted !== true) throw new ControlError("invalid_request", 400);
  return { client_request_id: input.client_request_id, name: input.name };
}

function parseLifecycleRequest(body: unknown): { client_request_id: string } {
  if (typeof body !== "object" || body === null || Array.isArray(body) ||
      typeof (body as Record<string, unknown>).client_request_id !== "string") {
    throw new ControlError("invalid_request", 400);
  }
  return { client_request_id: (body as { client_request_id: string }).client_request_id };
}

function parseServerId(body: unknown): string {
  const serverId = typeof body === "object" && body !== null ?
    (body as Record<string, unknown>).server_id : undefined;
  if (typeof serverId !== "string") throw new ControlError("invalid_request", 400);
  if (!UUID.test(serverId)) throw new ControlError("invalid_server_id", 400);
  return serverId;
}

function statusBody(server: ServerRecord) {
  return {
    server_id: server.id,
    name: server.logical_name,
    state: server.state,
    allocation_path: "cold",
    status_url: `/v1/servers/${server.id}`,
    ...(server.state === "running" && server.endpoint !== null ?
      { endpoint: server.endpoint } : {})
  };
}

function startRequestBody(request: StartRequestStatus) {
  return { start_request_id: request.id, server_id: request.server_id, state: request.state,
    arrived_at: request.arrived_at, arrival_order: request.arrival_order,
    valid_requests_ahead: request.valid_requests_ahead, reserved_at: request.reserved_at,
    expires_at: request.expires_at, admitted_at: request.admitted_at, terminal_at: request.terminal_at,
    queue_url: `/v1/start-requests/${request.id}` };
}

function errorBody(error: ControlError) {
  return error.code === "action_required" ?
    { error: error.code, action_required: "accept_eula" } : { error: error.code };
}

function sendError(response: ServerResponse, error: unknown): void {
  if (error instanceof ControlError) {
    sendJson(response, error.status, errorBody(error));
    return;
  }
  sendJson(response, 500, { error: "internal_error" });
}

const MCP_PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26"];
const MCP_STRING = { type: "string" };
const MCP_SERVER_ID = { type: "string", format: "uuid" };
const MCP_LIFECYCLE_SCHEMA = {
  type: "object",
  properties: { client_request_id: MCP_STRING, server_id: MCP_SERVER_ID },
  required: ["client_request_id", "server_id"]
};
const MCP_TOOLS = [
  {
    name: "minecraft_create",
    description: "Create the Paper Minecraft server asynchronously. Poll minecraft_status " +
      "until state is running; the endpoint is the playable address.",
    inputSchema: {
      type: "object",
      properties: {
        client_request_id: MCP_STRING,
        name: MCP_STRING,
        eula_accepted: { type: "boolean", description: "Must be true to accept the Minecraft EULA." }
      },
      required: ["client_request_id", "name", "eula_accepted"]
    }
  },
  {
    name: "minecraft_start",
    description: "Start a stopped server with its existing world. Asynchronous.",
    inputSchema: MCP_LIFECYCLE_SCHEMA
  },
  {
    name: "minecraft_stop",
    description: "Stop a server after saving its world. Asynchronous.",
    inputSchema: MCP_LIFECYCLE_SCHEMA
  },
  {
    name: "minecraft_status",
    description: "Report server state. Includes endpoint only once Minecraft is playable.",
    inputSchema: { type: "object", properties: { server_id: MCP_SERVER_ID }, required: ["server_id"] }
  }
];

function rpcError(id: unknown, code: number, message: string) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

async function callTool(control: ControlApi, principal: Principal, name: string, args: unknown) {
  if (name === "minecraft_create") {
    const input = parseCreateRequest(args);
    return control.mutate(principal.id, input.client_request_id, {
      operation: "create", name: input.name, eula_accepted: true
    });
  }
  if (name === "minecraft_start" || name === "minecraft_stop") {
    const serverId = parseServerId(args);
    const input = parseLifecycleRequest(args);
    return control.mutate(principal.id, input.client_request_id, {
      operation: name === "minecraft_start" ? "start" : "stop", server_id: serverId
    });
  }
  return statusBody(await control.getServer(principal.id, parseServerId(args)));
}

// Stateless Streamable HTTP: one JSON-RPC message per POST, answered as JSON.
async function handleMcp(message: unknown, principal: Principal, control: ControlApi) {
  if (typeof message !== "object" || message === null || Array.isArray(message) ||
      (message as Record<string, unknown>).jsonrpc !== "2.0") {
    return rpcError(null, -32600, "invalid_request");
  }
  const { id, method, params } = message as Record<string, unknown>;
  if (id === undefined || method === undefined) return null;
  if ((typeof id !== "string" && typeof id !== "number") || typeof method !== "string") {
    return rpcError(id, -32600, "invalid_request");
  }
  const input = typeof params === "object" && params !== null ? params as Record<string, unknown> : {};

  if (method === "initialize") {
    const requested = input.protocolVersion;
    return { jsonrpc: "2.0", id, result: {
      protocolVersion: typeof requested === "string" && MCP_PROTOCOL_VERSIONS.includes(requested) ?
        requested : MCP_PROTOCOL_VERSIONS[0],
      capabilities: { tools: {} },
      serverInfo: { name: "cloud-control", version: "0.0.0" }
    } };
  }
  if (method === "ping") return { jsonrpc: "2.0", id, result: {} };
  if (method === "tools/list") return { jsonrpc: "2.0", id, result: { tools: MCP_TOOLS } };
  if (method !== "tools/call") return rpcError(id, -32601, "method_not_found");

  const name = input.name;
  if (typeof name !== "string" || !MCP_TOOLS.some(tool => tool.name === name)) {
    return rpcError(id, -32602, "unknown_tool");
  }
  try {
    const result = await callTool(control, principal, name, input.arguments ?? {});
    return { jsonrpc: "2.0", id, result: {
      content: [{ type: "text", text: JSON.stringify(result) }], structuredContent: result
    } };
  } catch (error) {
    if (!(error instanceof ControlError)) return rpcError(id, -32603, "internal_error");
    const body = errorBody(error);
    return { jsonrpc: "2.0", id, result: {
      content: [{ type: "text", text: JSON.stringify(body) }], structuredContent: body, isError: true
    } };
  }
}

export function handleRequest(
  request: IncomingMessage,
  response: ServerResponse
): void {
  if (request.method === "GET" && request.url === "/healthz") {
    sendJson(response, 200, { service: "cloud-control", status: "ok" });
    return;
  }

  sendJson(response, 404, { error: "not_found" });
}

export function createRequestHandler(control: ControlApi) {
  return async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    try {
      const path = new URL(request.url ?? "/", "http://localhost").pathname;
      if (request.method === "GET" && path === "/healthz") {
        sendJson(response, 200, { service: "cloud-control", status: "ok" });
        return;
      }

      if (request.method === "GET" && path === "/v1/whoami") {
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        sendJson(response, 200, { id: principal.id, subject: principal.subject });
        return;
      }

      if (request.method === "POST" && path === "/v1/servers") {
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        const input = parseCreateRequest(await readJson(request));
        const accepted = await control.mutate(principal.id, input.client_request_id, {
          operation: "create", name: input.name, eula_accepted: true
        });
        sendJson(response, 202, accepted);
        return;
      }

      const queue = /^\/v1\/start-requests\/([^/]+)(\/cancel)?$/.exec(path);
      if (queue !== null && ((request.method === "GET" && queue[2] === undefined) ||
                            (request.method === "POST" && queue[2] === "/cancel"))) {
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        const requestId = queue[1]!;
        if (!UUID.test(requestId)) throw new ControlError("invalid_start_request_id", 400);
        if (request.method === "GET" && control.getStartRequest !== undefined) {
          sendJson(response, 200, startRequestBody(await control.getStartRequest(principal.id, requestId)));
        } else if (request.method === "POST" && control.cancelStartRequest !== undefined) {
          const input = parseLifecycleRequest(await readJson(request));
          sendJson(response, 202, await control.cancelStartRequest(principal.id, requestId, input.client_request_id));
        } else throw new ControlError("start_request_not_found", 404);
        return;
      }

      const lifecycle = request.method === "POST" ?
        /^\/v1\/servers\/([^/]+)\/(start|stop)$/.exec(path) : null;
      if (lifecycle !== null) {
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        const serverId = lifecycle[1]!;
        if (!UUID.test(serverId)) throw new ControlError("invalid_server_id", 400);
        const input = parseLifecycleRequest(await readJson(request));
        const accepted = await control.mutate(principal.id, input.client_request_id, {
          operation: lifecycle[2] as "start" | "stop", server_id: serverId
        });
        sendJson(response, 202, accepted);
        return;
      }

      const match = request.method === "GET" ? /^\/v1\/servers\/([^/]+)$/.exec(path) : null;
      if (match !== null) {
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        const serverId = match[1]!;
        if (!UUID.test(serverId)) throw new ControlError("invalid_server_id", 400);
        sendJson(response, 200, statusBody(await control.getServer(principal.id, serverId)));
        return;
      }

      if (path === "/mcp") {
        if (request.method !== "POST") {
          response.setHeader("allow", "POST");
          sendJson(response, 405, { error: "method_not_allowed" });
          return;
        }
        const principal = await requirePrincipal(request, response, control);
        if (principal === null) return;
        let message: unknown;
        try {
          message = await readJson(request);
        } catch (error) {
          if (!(error instanceof ControlError)) throw error;
          sendJson(response, error.status,
            rpcError(null, error.code === "invalid_json" ? -32700 : -32600, error.code));
          return;
        }
        const reply = await handleMcp(message, principal, control);
        if (reply === null) {
          response.writeHead(202);
          response.end();
        } else {
          sendJson(response, 200, reply);
        }
        return;
      }

      sendJson(response, 404, { error: "not_found" });
    } catch (error) {
      sendError(response, error);
    }
  };
}
