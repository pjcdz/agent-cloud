import assert from "node:assert/strict";
import pg from "pg";
import { ControlStore } from "../../src/database.js";
import { ControlError, type Mutation } from "../../src/lifecycle.js";

export interface QueueCommand {
  action: "mutate" | "get";
  principalId: string;
  requestId: string;
  mutation?: Mutation;
}
const [schema, token] = process.argv.slice(2);
assert.ok(schema && /^test_[a-f0-9]{32}$/.test(schema));
assert.ok(token && process.send && process.env.TEST_DATABASE_URL);
const store = new ControlStore(new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL,
  options: `-c search_path=${schema}` }), 0, null, true);
await store.initialize(token);
process.on("message", async (message) => {
  const command = message as QueueCommand;
  try {
    const result = command.action === "get" ? await store.getStartRequest(command.principalId, command.requestId) :
      await store.mutate(command.principalId, command.requestId, command.mutation!);
    process.send!({ result });
  } catch (error) {
    process.send!({ error: error instanceof ControlError ? error.code : String(error) });
  }
});
process.send!({ ready: true });
