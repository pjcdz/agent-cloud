import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";

import { createRequestHandler, handleRequest } from "../src/app.js";
import { ControlError, type Mutation, type ServerRecord } from "../src/lifecycle.js";
import type { StartRequestStatus } from "../src/start-requests.js";

const SERVER_ID = "44752f9d-5564-4e43-9346-cd30a99a1613";
const START_REQUEST_ID = "84752f9d-5564-4e43-9346-cd30a99a1613";

async function withServer(run: (origin: string) => Promise<void>): Promise<void> {
  const server = createServer(handleRequest);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");

  const address = server.address();
  assert(address !== null && typeof address !== "string");

  try {
    await run(`http://127.0.0.1:${address.port}`);
  } finally {
    server.close();
    await once(server, "close");
  }
}

async function withAuthenticatedServer(
  control: Parameters<typeof createRequestHandler>[0],
  run: (origin: string) => Promise<void>
): Promise<void> {
  const server = createServer(createRequestHandler(control));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert(address !== null && typeof address !== "string");
  try {
    await run(`http://127.0.0.1:${address.port}`);
  } finally {
    server.close();
    await once(server, "close");
  }
}

test("GET /healthz reports cloud-control health", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/healthz`);

    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      service: "cloud-control",
      status: "ok"
    });
  });
});

test("unknown routes return a typed 404", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/missing`);

    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { error: "not_found" });
  });
});

test("machine identity requires a valid bearer token", async () => {
  const control = createControl().control;
  await withAuthenticatedServer(control, async (origin) => {
    const missing = await fetch(`${origin}/v1/whoami`);
    assert.equal(missing.status, 401);

    const invalid = await fetch(`${origin}/v1/whoami`, {
      headers: { authorization: "Bearer wrong" }
    });
    assert.equal(invalid.status, 401);

    const valid = await fetch(`${origin}/v1/whoami`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.equal(valid.status, 200);
    assert.deepEqual(await valid.json(), { id: "principal-1", subject: "operator" });
  });
});

test("queue backend authenticates reads and cancellation and returns durable state without an ETA", async () => {
  const fake = createControl();
  const request: StartRequestStatus = {
    id: START_REQUEST_ID, principal_id: "principal-1", server_id: SERVER_ID, client_request_id: "start-one",
    server_generation: 1, arrival_order: "123", arrived_at: new Date("2026-10-06T12:00:00Z"),
    state: "queued", valid_requests_ahead: 2, reserved_at: null, expires_at: null,
    admitted_at: null, terminal_at: null
  };
  const control = { ...fake.control,
    getStartRequest: async (principalId: string, requestId: string) => {
      assert.equal(principalId, request.principal_id);
      if (requestId !== request.id) throw new ControlError("start_request_not_found", 404);
      return request;
    },
    cancelStartRequest: async (principalId: string, requestId: string, clientRequestId: string) => {
      assert.equal(principalId, request.principal_id);
      assert.equal(clientRequestId, "cancel-one");
      if (requestId !== request.id) throw new ControlError("start_request_not_found", 404);
      return { start_request_id: requestId, server_id: SERVER_ID, state: "cancelled" as const,
        queue_url: `/v1/start-requests/${requestId}` };
    }
  };
  await withAuthenticatedServer(control, async origin => {
    const url = `${origin}/v1/start-requests/${START_REQUEST_ID}`;
    const headers = { authorization: "Bearer operator-machine-token", "content-type": "application/json" };
    assert.equal((await fetch(url)).status, 401);
    assert.equal((await fetch(`${url}/cancel`, { method: "POST" })).status, 401);
    const response = await fetch(url, { headers });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.deepEqual(body, {
      start_request_id: START_REQUEST_ID, server_id: SERVER_ID, state: "queued",
      arrived_at: "2026-10-06T12:00:00.000Z", arrival_order: "123", valid_requests_ahead: 2,
      reserved_at: null, expires_at: null, admitted_at: null, terminal_at: null,
      queue_url: `/v1/start-requests/${START_REQUEST_ID}`
    });
    assert.deepEqual(await (await fetch(url, { headers })).json(), body);
    assert.equal((await fetch(`${origin}/v1/start-requests/${SERVER_ID}`, { headers })).status, 404);
    assert.equal((await fetch(`${origin}/v1/start-requests/invalid`, { headers })).status, 400);
    const cancelled = await fetch(`${url}/cancel`, { method: "POST", headers,
      body: JSON.stringify({ client_request_id: "cancel-one" }) });
    assert.equal(cancelled.status, 202);
    assert.deepEqual(await cancelled.json(), { start_request_id: START_REQUEST_ID, server_id: SERVER_ID,
      state: "cancelled", queue_url: `/v1/start-requests/${START_REQUEST_ID}` });
  });
});

function createControl() {
  let server: ServerRecord = {
    id: SERVER_ID,
    principal_id: "principal-1",
    logical_name: "one",
    recipe_id: "paper-26.2-121",
    profile_id: "two-player",
    world_identity: `cloud-world-${SERVER_ID}`,
    desired_state: "running",
    state: "queued",
    generation: 1,
    observed_generation: 0,
    last_request_id: "create-one",
    endpoint: null
  };
  const mutations: Mutation[] = [];
  return {
    control: {
      authenticate: async (token: string) => token === "operator-machine-token"
        ? { id: "principal-1", subject: "operator" }
        : null,
      mutate: async (principalId: string, requestId: string, mutation: Mutation) => {
        assert.equal(principalId, "principal-1");
        if (mutation.operation !== "create" && mutation.server_id !== SERVER_ID) {
          throw new ControlError("server_not_found", 404);
        }
        mutations.push(mutation);
        return { request_id: requestId, server_id: SERVER_ID, state: "queued" as const,
          status_url: `/v1/servers/${SERVER_ID}` };
      },
      getServer: async (principalId: string, serverId: string) => {
        if (principalId !== "principal-1" || serverId !== SERVER_ID) {
          throw new ControlError("server_not_found", 404);
        }
        return server;
      }
    },
    mutations,
    setRunning() {
      server = { ...server, state: "running", observed_generation: 1,
        endpoint: { host: "game.invalid", port: 30_001 } };
    }
  };
}

test("REST create is asynchronous and status exposes only a ready endpoint", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const created = await fetch(`${origin}/v1/servers`, {
      method: "POST",
      headers: {
        authorization: "Bearer operator-machine-token",
        "content-type": "application/json"
      },
      body: JSON.stringify({ client_request_id: "create-one", name: "one", eula_accepted: true })
    });
    assert.equal(created.status, 202);
    assert.deepEqual(await created.json(), {
      request_id: "create-one",
      server_id: SERVER_ID,
      state: "queued",
      status_url: `/v1/servers/${SERVER_ID}`
    });
    assert.deepEqual(fake.mutations, [{ operation: "create", name: "one", eula_accepted: true }]);

    const first = await fetch(`${origin}/v1/servers/${SERVER_ID}`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    const second = await fetch(`${origin}/v1/servers/${SERVER_ID}`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.equal(first.status, 200);
    assert.deepEqual(await first.json(), await second.json());
  });
});

test("REST status gates endpoint readiness and ownership", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const queued = await fetch(`${origin}/v1/servers/${SERVER_ID}`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.deepEqual(await queued.json(), {
      server_id: SERVER_ID,
      name: "one",
      state: "queued",
      allocation_path: "cold",
      status_url: `/v1/servers/${SERVER_ID}`
    });

    fake.setRunning();
    const running = await fetch(`${origin}/v1/servers/${SERVER_ID}`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.deepEqual(await running.json(), {
      server_id: SERVER_ID,
      name: "one",
      state: "running",
      allocation_path: "cold",
      status_url: `/v1/servers/${SERVER_ID}`,
      endpoint: { host: "game.invalid", port: 30_001 }
    });

    const missing = await fetch(`${origin}/v1/servers/22e65178-a174-4ef3-8434-18765925c08f`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.equal(missing.status, 404);
    assert.deepEqual(await missing.json(), { error: "server_not_found" });
  });
});

test("REST create validates authentication, JSON size, fields and EULA", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const cases: Array<[RequestInit, number, object]> = [
      [{ method: "POST", headers: { "content-type": "application/json" }, body: "{}" },
        401, { error: "unauthorized" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token" }, body: "{}" },
        415, { error: "unsupported_media_type" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token",
        "content-type": "application/json" }, body: "{" }, 400, { error: "invalid_json" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token",
        "content-type": "application/json" }, body: JSON.stringify({ client_request_id: "create-one",
          name: "one", eula_accepted: false }) }, 409,
        { error: "action_required", action_required: "accept_eula" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token",
        "content-type": "application/json" }, body: JSON.stringify({ client_request_id: "create-one",
          name: "one" }) }, 409, { error: "action_required", action_required: "accept_eula" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token",
        "content-type": "application/json" }, body: JSON.stringify({ client_request_id: 1,
          name: "one", eula_accepted: true }) }, 400, { error: "invalid_request" }],
      [{ method: "POST", headers: { authorization: "Bearer operator-machine-token",
        "content-type": "application/json" }, body: JSON.stringify({ client_request_id: "create-large",
          name: "x".repeat(17_000), eula_accepted: true }) }, 413, { error: "request_too_large" }]
    ];
    for (const [request, status, body] of cases) {
      const response = await fetch(`${origin}/v1/servers`, request);
      assert.equal(response.status, status);
      assert.deepEqual(await response.json(), body);
    }
    const invalidId = await fetch(`${origin}/v1/servers/not-a-uuid`, {
      headers: { authorization: "Bearer operator-machine-token" }
    });
    assert.equal(invalidId.status, 400);
    assert.deepEqual(await invalidId.json(), { error: "invalid_server_id" });
    assert.equal(fake.mutations.length, 0);
  });
});

test("REST start and stop record asynchronous intent for an owned server", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const auth = { authorization: "Bearer operator-machine-token", "content-type": "application/json" };
    for (const operation of ["stop", "start"] as const) {
      const response = await fetch(`${origin}/v1/servers/${SERVER_ID}/${operation}`, {
        method: "POST", headers: auth, body: JSON.stringify({ client_request_id: `${operation}-one` })
      });
      assert.equal(response.status, 202);
      assert.deepEqual(await response.json(), {
        request_id: `${operation}-one`,
        server_id: SERVER_ID,
        state: "queued",
        status_url: `/v1/servers/${SERVER_ID}`
      });
    }
    assert.deepEqual(fake.mutations, [
      { operation: "stop", server_id: SERVER_ID },
      { operation: "start", server_id: SERVER_ID }
    ]);
  });
});

test("REST start and stop validate authentication, identifiers and body", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const auth = { authorization: "Bearer operator-machine-token", "content-type": "application/json" };
    const body = JSON.stringify({ client_request_id: "stop-one" });
    const cases: Array<[string, RequestInit, number, object]> = [
      [SERVER_ID, { method: "POST", headers: { "content-type": "application/json" }, body },
        401, { error: "unauthorized" }],
      ["not-a-uuid", { method: "POST", headers: auth, body }, 400, { error: "invalid_server_id" }],
      [SERVER_ID, { method: "POST", headers: { authorization: auth.authorization }, body },
        415, { error: "unsupported_media_type" }],
      [SERVER_ID, { method: "POST", headers: auth, body: "{" }, 400, { error: "invalid_json" }],
      [SERVER_ID, { method: "POST", headers: auth, body: "{}" }, 400, { error: "invalid_request" }],
      [SERVER_ID, { method: "POST", headers: auth, body: JSON.stringify({ client_request_id: 1 }) },
        400, { error: "invalid_request" }],
      ["22e65178-a174-4ef3-8434-18765925c08f", { method: "POST", headers: auth, body },
        404, { error: "server_not_found" }]
    ];
    for (const [serverId, request, status, expected] of cases) {
      const response = await fetch(`${origin}/v1/servers/${serverId}/stop`, request);
      assert.equal(response.status, status);
      assert.deepEqual(await response.json(), expected);
    }
    const wrongMethod = await fetch(`${origin}/v1/servers/${SERVER_ID}/start`, {
      headers: { authorization: auth.authorization }
    });
    assert.equal(wrongMethod.status, 404);
    assert.equal(fake.mutations.length, 0);
  });
});

async function mcp(origin: string, message: object, token = "operator-machine-token") {
  return fetch(`${origin}/mcp`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
      accept: "application/json, text/event-stream"
    },
    body: JSON.stringify(message)
  });
}

async function callTool(origin: string, name: string, args: object) {
  const response = await mcp(origin, {
    jsonrpc: "2.0", id: name, method: "tools/call", params: { name, arguments: args }
  });
  assert.equal(response.status, 200);
  const reply = await response.json() as { id: string; result: {
    content: Array<{ type: string; text: string }>; structuredContent: unknown; isError?: boolean
  } };
  assert.equal(reply.id, name);
  assert.deepEqual(JSON.parse(reply.result.content[0]!.text), reply.result.structuredContent);
  return reply.result;
}

test("MCP initializes and lists the four Minecraft tools", async () => {
  await withAuthenticatedServer(createControl().control, async (origin) => {
    const initialized = await mcp(origin, { jsonrpc: "2.0", id: 1, method: "initialize",
      params: { protocolVersion: "2025-03-26", capabilities: {},
        clientInfo: { name: "test", version: "1" } } });
    assert.deepEqual(await initialized.json(), { jsonrpc: "2.0", id: 1, result: {
      protocolVersion: "2025-03-26",
      capabilities: { tools: {} },
      serverInfo: { name: "cloud-control", version: "0.0.0" }
    } });

    const unknownVersion = await mcp(origin, { jsonrpc: "2.0", id: 2, method: "initialize",
      params: { protocolVersion: "1999-01-01" } });
    assert.equal(((await unknownVersion.json()) as { result: { protocolVersion: string } })
      .result.protocolVersion, "2025-06-18");

    const notified = await mcp(origin, { jsonrpc: "2.0", method: "notifications/initialized" });
    assert.equal(notified.status, 202);
    assert.equal(await notified.text(), "");

    const listed = await mcp(origin, { jsonrpc: "2.0", id: 3, method: "tools/list" });
    const tools = ((await listed.json()) as { result: { tools: Array<{ name: string }> } }).result.tools;
    assert.deepEqual(tools.map(tool => tool.name),
      ["minecraft_create", "minecraft_start", "minecraft_stop", "minecraft_status"]);
  });
});

test("MCP tools run the shared lifecycle and gate the endpoint on readiness", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const accepted = (operation: string) => ({ request_id: `${operation}-one`, server_id: SERVER_ID,
      state: "queued", status_url: `/v1/servers/${SERVER_ID}` });

    const created = await callTool(origin, "minecraft_create",
      { client_request_id: "create-one", name: "one", eula_accepted: true });
    assert.equal(created.isError, undefined);
    assert.deepEqual(created.structuredContent, accepted("create"));

    const queued = await callTool(origin, "minecraft_status", { server_id: SERVER_ID });
    assert.deepEqual(queued.structuredContent, { server_id: SERVER_ID, name: "one", state: "queued",
      allocation_path: "cold", status_url: `/v1/servers/${SERVER_ID}` });

    fake.setRunning();
    const running = await callTool(origin, "minecraft_status", { server_id: SERVER_ID });
    assert.deepEqual((running.structuredContent as { endpoint: unknown }).endpoint,
      { host: "game.invalid", port: 30_001 });

    for (const operation of ["stop", "start"]) {
      const result = await callTool(origin, `minecraft_${operation}`,
        { client_request_id: `${operation}-one`, server_id: SERVER_ID });
      assert.deepEqual(result.structuredContent, accepted(operation));
    }
    assert.deepEqual(fake.mutations, [
      { operation: "create", name: "one", eula_accepted: true },
      { operation: "stop", server_id: SERVER_ID },
      { operation: "start", server_id: SERVER_ID }
    ]);
  });
});

test("MCP reports control errors as tool errors without mutating", async () => {
  const fake = createControl();
  await withAuthenticatedServer(fake.control, async (origin) => {
    const cases: Array<[string, object, object]> = [
      ["minecraft_create", { client_request_id: "create-one", name: "one" },
        { error: "action_required", action_required: "accept_eula" }],
      ["minecraft_create", { name: "one", eula_accepted: true }, { error: "invalid_request" }],
      ["minecraft_stop", { server_id: SERVER_ID }, { error: "invalid_request" }],
      ["minecraft_start", { client_request_id: "start-one" }, { error: "invalid_request" }],
      ["minecraft_start", { client_request_id: "start-one", server_id: "not-a-uuid" },
        { error: "invalid_server_id" }],
      ["minecraft_status", { server_id: "22e65178-a174-4ef3-8434-18765925c08f" },
        { error: "server_not_found" }]
    ];
    for (const [name, args, expected] of cases) {
      const result = await callTool(origin, name, args);
      assert.equal(result.isError, true);
      assert.deepEqual(result.structuredContent, expected);
    }
    assert.equal(fake.mutations.length, 0);
  });
});

test("MCP requires the machine token and valid JSON-RPC", async () => {
  await withAuthenticatedServer(createControl().control, async (origin) => {
    const unauthorized = await mcp(origin, { jsonrpc: "2.0", id: 1, method: "tools/list" }, "wrong");
    assert.equal(unauthorized.status, 401);
    assert.equal(unauthorized.headers.get("www-authenticate"), "Bearer");

    const get = await fetch(`${origin}/mcp`, { headers: { authorization: "Bearer operator-machine-token" } });
    assert.equal(get.status, 405);

    const parse = await fetch(`${origin}/mcp`, { method: "POST", body: "{",
      headers: { authorization: "Bearer operator-machine-token", "content-type": "application/json" } });
    assert.equal(parse.status, 400);
    assert.deepEqual(await parse.json(),
      { jsonrpc: "2.0", id: null, error: { code: -32700, message: "invalid_json" } });

    const cases: Array<[object, object]> = [
      [[{ jsonrpc: "2.0", id: 1, method: "ping" }], { id: null, error: { code: -32600, message: "invalid_request" } }],
      [{ jsonrpc: "2.0", id: 2, method: "resources/list" }, { id: 2, error: { code: -32601, message: "method_not_found" } }],
      [{ jsonrpc: "2.0", id: 3, method: "tools/call", params: { name: "docker_exec" } },
        { id: 3, error: { code: -32602, message: "unknown_tool" } }],
      [{ jsonrpc: "2.0", id: 4, method: "ping" }, { id: 4, result: {} }]
    ];
    for (const [message, expected] of cases) {
      const response = await mcp(origin, message);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), { jsonrpc: "2.0", ...expected });
    }
  });
});
