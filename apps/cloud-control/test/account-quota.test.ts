import assert from "node:assert/strict";
import test from "node:test";

import { ControlStore } from "../src/database.js";

test("account quota configuration accepts only the approved beta policy", async () => {
  const environment = {
    DATABASE_HOST: "127.0.0.1", DATABASE_NAME: "unused", DATABASE_USER: "unused", DATABASE_PASSWORD: "unused"
  };
  for (const value of ["", "true", "2", "unknown"]) {
    assert.throws(() => ControlStore.fromEnvironment({ ...environment, CLOUD_ACCOUNT_QUOTA: value }),
      /CLOUD_ACCOUNT_QUOTA must be beta when set/);
  }
  for (const value of [undefined, "beta"]) {
    const store = ControlStore.fromEnvironment({ ...environment, CLOUD_ACCOUNT_QUOTA: value });
    await store.close();
  }
});
