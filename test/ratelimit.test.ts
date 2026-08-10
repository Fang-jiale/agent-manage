import { test } from "node:test";
import assert from "node:assert/strict";
import { RateLimiter, clientIp } from "../src/ratelimit.ts";

test("sliding window allows then blocks", () => {
  const rl = new RateLimiter(3, 1000);
  assert.equal(rl.allow("k"), true);
  assert.equal(rl.allow("k"), true);
  assert.equal(rl.allow("k"), true);
  assert.equal(rl.allow("k"), false);
  // 其他 key 不受影响
  assert.equal(rl.allow("other"), true);
});

test("window expires", async () => {
  const rl = new RateLimiter(1, 30);
  assert.equal(rl.allow("k"), true);
  assert.equal(rl.allow("k"), false);
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(rl.allow("k"), true);
});

test("clientIp prefers x-forwarded-for", () => {
  assert.equal(clientIp({ "x-forwarded-for": "1.2.3.4, 10.0.0.1" }, "127.0.0.1"), "1.2.3.4");
  assert.equal(clientIp({ "x-forwarded-for": ["5.6.7.8"] }, "127.0.0.1"), "5.6.7.8");
  assert.equal(clientIp({}, "127.0.0.1"), "127.0.0.1");
  assert.equal(clientIp({}, undefined), "unknown");
});
