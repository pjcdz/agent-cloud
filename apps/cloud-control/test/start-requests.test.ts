import assert from "node:assert/strict";
import test from "node:test";

import { ControlStore } from "../src/database.js";

test("start queue is opt-in and accepts only the approved beta profile policy", async () => {
  const environment = {
    DATABASE_HOST: "127.0.0.1", DATABASE_NAME: "unused", DATABASE_USER: "unused", DATABASE_PASSWORD: "unused"
  };
  for (const value of ["", "true", "2", "unknown"]) {
    assert.throws(() => ControlStore.fromEnvironment({ ...environment, CLOUD_START_QUEUE: value }),
      /CLOUD_START_QUEUE must be beta when set/);
  }
  for (const value of [undefined, "beta"]) {
    const store = ControlStore.fromEnvironment({ ...environment, CLOUD_START_QUEUE: value });
    await store.close();
  }
});
