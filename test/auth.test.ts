import { test } from "node:test";
import assert from "node:assert/strict";
import { hashPassword, verifyPassword, signJwt, verifyJwt } from "../src/auth.ts";

test("password hash roundtrip", () => {
  const stored = hashPassword("s3cret");
  assert.ok(stored.startsWith("scrypt:"));
  assert.equal(verifyPassword("s3cret", stored), true);
  assert.equal(verifyPassword("wrong", stored), false);
});

test("verifyPassword rejects malformed stored hash", () => {
  assert.equal(verifyPassword("x", ""), false);
  assert.equal(verifyPassword("x", "plain"), false);
  assert.equal(verifyPassword("x", "bcrypt:aa:bb"), false);
});

test("same password produces different salts", () => {
  assert.notEqual(hashPassword("pw"), hashPassword("pw"));
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
