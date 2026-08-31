import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { hashPassword, verifyPassword, passwordNeedsRehash, signJwt, verifyJwt } from "../src/auth.ts";

test("password hash roundtrip (scrypt2 with embedded params)", async () => {
  const stored = await hashPassword("s3cret");
  const parts = stored.split(":");
  assert.equal(parts[0], "scrypt2");
  assert.equal(Number(parts[1]), 2 ** 17); // OWASP 级参数入格式
  assert.equal(Number(parts[2]), 8);
  assert.equal(Number(parts[3]), 1);
  assert.equal(await verifyPassword("s3cret", stored), true);
  assert.equal(await verifyPassword("wrong", stored), false);
  assert.equal(passwordNeedsRehash(stored), false);
});

test("legacy scrypt format still verifies and is flagged for rehash", async () => {
  // 旧格式（Node 默认参数 N=16384）：历史库存量条目，验证通过后登录路径会静默升级
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync("s3cret", salt, 64).toString("hex");
  const stored = `scrypt:${salt}:${hash}`;
  assert.equal(await verifyPassword("s3cret", stored), true);
  assert.equal(await verifyPassword("wrong", stored), false);
  assert.equal(passwordNeedsRehash(stored), true);
});

test("verifyPassword rejects malformed stored hash", async () => {
  assert.equal(await verifyPassword("x", ""), false);
  assert.equal(await verifyPassword("x", "plain"), false);
  assert.equal(await verifyPassword("x", "bcrypt:aa:bb"), false);
  assert.equal(await verifyPassword("x", "scrypt2:not:num:eric:aa:bb"), false);
  // 参数越界（防库里被塞入天文数字参数卡死线程池）
  assert.equal(await verifyPassword("x", "scrypt2:999999999:8:1:aa:bb"), false);
  assert.equal(await verifyPassword("x", "scrypt2:1048576:8:1:aa:bb"), false);
});

test("same password produces different salts", async () => {
  assert.notEqual(await hashPassword("pw"), await hashPassword("pw"));
});

test("jwt sign/verify roundtrip", () => {
  const token = signJwt({ sub: "u-1", name: "admin" }, "secret", 60_000);
  const claims = verifyJwt(token, "secret");
  assert.equal(claims?.sub, "u-1");
  assert.equal(claims?.name, "admin");
  assert.ok(typeof claims?.iat === "number" && typeof claims?.exp === "number");
});

test("jwt rejected with wrong secret", () => {
  const token = signJwt({ sub: "u-1", name: "admin" }, "secret", 60_000);
  assert.equal(verifyJwt(token, "other-secret"), undefined);
});

test("jwt rejected when expired", () => {
  const token = signJwt({ sub: "u-1", name: "admin" }, "secret", -1000);
  assert.equal(verifyJwt(token, "secret"), undefined);
});

test("jwt rejected when payload tampered", () => {
  const token = signJwt({ sub: "u-1", name: "admin" }, "secret", 60_000);
  const [h, , s] = token.split(".");
  const forgedBody = Buffer.from(JSON.stringify({ sub: "u-evil", name: "evil", iat: 1, exp: 9999999999 })).toString("base64url");
  assert.equal(verifyJwt(`${h}.${forgedBody}.${s}`, "secret"), undefined);
});

test("jwt rejected when malformed", () => {
  assert.equal(verifyJwt("", "secret"), undefined);
  assert.equal(verifyJwt("a.b", "secret"), undefined);
  assert.equal(verifyJwt("a.b.c.d", "secret"), undefined);
  assert.equal(verifyJwt("!!.!!.!!", "secret"), undefined);
});
