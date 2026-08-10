import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db } from "../src/db.ts";
import { setLogLevel } from "../src/util.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const KID = "mock-key-1";

// 最小 mock IdP：discovery + authorize（自动放行）+ token（PKCE 校验）+ JWKS
interface MockIdP {
  issuer: string;
  close: () => Promise<void>;
}

async function startMockIdP(): Promise<MockIdP> {
  const { privateKey, publicKey } = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256", use: "sig" };
  // 记录每次授权的 challenge/nonce，token 端点据此签发
  const grants = new Map<string, { challenge: string; nonce: string; redirectURI: string }>();

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const issuer = `http://localhost:${(server.address() as AddressInfo).port}`;
    if (url.pathname === "/.well-known/openid-configuration") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        issuer,
        authorization_endpoint: `${issuer}/authorize`,
        token_endpoint: `${issuer}/token`,
        jwks_uri: `${issuer}/jwks`,
      }));
      return;
    }
    if (url.pathname === "/jwks") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    if (url.pathname === "/authorize") {
      const code = "code-" + crypto.randomBytes(8).toString("hex");
      grants.set(code, {
        challenge: url.searchParams.get("code_challenge") ?? "",
        nonce: url.searchParams.get("nonce") ?? "",
        redirectURI: url.searchParams.get("redirect_uri") ?? "",
      });
      const back = new URL(url.searchParams.get("redirect_uri") ?? "");
      back.searchParams.set("code", code);
      back.searchParams.set("state", url.searchParams.get("state") ?? "");
      res.writeHead(302, { Location: back.toString() });
      res.end();
      return;
    }
    if (url.pathname === "/token" && req.method === "POST") {
      void (async () => {
        const raw = await new Promise<string>((resolve) => {
          let b = "";
          req.on("data", (c) => b += c);
          req.on("end", () => resolve(b));
        });
        const body = new URLSearchParams(raw);
        const grant = grants.get(body.get("code") ?? "");
        const fail = (msg: string) => {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: msg }));
        };
        if (!grant) return fail("unknown code");
        const auth = req.headers.authorization ?? "";
        const [cid, csecret] = Buffer.from(auth.replace("Basic ", ""), "base64").toString().split(":");
        if (cid !== "test-client" || csecret !== "test-secret") return fail("bad client");
        const digest = crypto.createHash("sha256").update(body.get("code_verifier") ?? "").digest("base64url");
        if (digest !== grant.challenge) return fail("pkce mismatch");

        const now = Math.floor(Date.now() / 1000);
        const header = Buffer.from(JSON.stringify({ alg: "RS256", kid: KID })).toString("base64url");
        const payload = Buffer.from(JSON.stringify({
          iss: issuer,
          aud: "test-client",
          sub: "idp-sub-000123456",
          employee_id: "000123456",
          name: "张三",
          nonce: grant.nonce,
          iat: now,
          exp: now + 600,
        })).toString("base64url");
        const sig = crypto.sign("RSA-SHA256", Buffer.from(`${header}.${payload}`), privateKey).toString("base64url");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ id_token: `${header}.${payload}.${sig}`, access_token: "at", token_type: "Bearer" }));
      })();
      return;
    }
    res.writeHead(404).end("not found");
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    issuer: `http://localhost:${port}`,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

function testConfig(issuer: string, gatewayPort?: number): GatewayConfig {
  return {
    addr: ":0",
    logLevel: "error",
    agentTimeoutMs: 90_000,
    userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000,
    databaseURL: "",
    jwtSecret: "oidc-test-secret",
    jwtTtlMs: 3_600_000,
    adminPassword: "x",
    redisURL: "",
    redisPrefix: "ywm",
    instanceID: "oidc-test",
    attachDir: "",
    attachQuotaMb: 0,
    retentionDays: 0,
    s3Endpoint: "",
    s3Region: "us-east-1",
    s3Bucket: "ywmatrix",
    s3AccessKey: "",
    s3SecretKey: "",
    s3PublicURL: "",
    oidcIssuer: issuer,
    oidcClientID: "test-client",
    oidcClientSecret: "test-secret",
    oidcRedirectURL: `http://localhost:${gatewayPort ?? 0}/auth/oidc/callback`,
    oidcEmployeeClaim: "employee_id",
  };
}

test("oidc full flow with mock IdP", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 OIDC 测试");
    await db.close().catch(() => {});
    return;
  }
  const idp = await startMockIdP();

  // redirect_uri 需要网关端口，先起网关拿端口再重建配置太绕：
  // 直接预监听一个端口，再用它构造配置
  const probe = http.createServer();
  await new Promise<void>((resolve) => probe.listen(0, resolve));
  const gatewayPort = (probe.address() as AddressInfo).port;
  await new Promise<void>((resolve) => probe.close(() => resolve()));

  const cfg = testConfig(idp.issuer, gatewayPort);
  const { server } = await createGatewayServer(cfg, STATIC_FILE, db);
  await new Promise<void>((resolve) => server.listen(gatewayPort, resolve));
  const base = `http://localhost:${gatewayPort}`;
  const createdUserIds: string[] = [];
  try {
    // /auth/config 暴露 OIDC 开关
    const conf = await (await fetch(`${base}/auth/config`)).json() as { oidc: boolean };
    assert.equal(conf.oidc, true);

    // 登录跳转 → mock IdP 授权 → 回调
    const loginResp = await fetch(`${base}/auth/oidc/login`, { redirect: "manual" });
    assert.equal(loginResp.status, 302);
    const authURL = loginResp.headers.get("location") ?? "";
    assert.ok(authURL.startsWith(`${idp.issuer}/authorize?`));

    const authResp = await fetch(authURL, { redirect: "manual" });
    assert.equal(authResp.status, 302);
    const callbackURL = authResp.headers.get("location") ?? "";
    assert.ok(callbackURL.startsWith(`${base}/auth/oidc/callback?`));

    const cbResp = await fetch(callbackURL);
    assert.equal(cbResp.status, 200);
    const html = await cbResp.text();
    const m = html.match(/s\.token = "([^"]+)"/);
    assert.ok(m, "callback page should embed token");
    const token = m[1];

    // token 可用于 /ws/admin；用户已按工号自动建号
    await new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(`ws://localhost:${gatewayPort}/ws/admin?token=${token}`);
      ws.once("open", () => { ws.close(); resolve(); });
      ws.once("error", reject);
    });
    const user = await db.getUserByEmployeeID("000123456");
    assert.ok(user);
    assert.equal(user.name, "张三");
    assert.equal(user.display_name, "张三");
    assert.equal(user.role, "user");
    createdUserIds.push(user.id);

    // 再次登录同一工号 → 同一账号，不重复建号
    const login2 = await fetch(`${base}/auth/oidc/login`, { redirect: "manual" });
    const auth2 = await fetch(login2.headers.get("location") ?? "", { redirect: "manual" });
    const cb2 = await fetch(auth2.headers.get("location") ?? "");
    assert.equal(cb2.status, 200);
    const users = (await db.listUsers()).filter((u) => u.employee_id === "000123456");
    assert.equal(users.length, 1);

    // 篡改 state → 400
    const login3 = await fetch(`${base}/auth/oidc/login`, { redirect: "manual" });
    const auth3 = await fetch(login3.headers.get("location") ?? "", { redirect: "manual" });
    const bad = new URL(auth3.headers.get("location") ?? "");
    bad.searchParams.set("state", bad.searchParams.get("state")?.slice(0, -2) + "xx");
    const cb3 = await fetch(bad);
    assert.equal(cb3.status, 400);

    // 禁用后 OIDC 登录同样被拒
    await db.setUserDisabled(user.id, true);
    const login4 = await fetch(`${base}/auth/oidc/login`, { redirect: "manual" });
    const auth4 = await fetch(login4.headers.get("location") ?? "", { redirect: "manual" });
    const cb4 = await fetch(auth4.headers.get("location") ?? "");
    assert.equal(cb4.status, 400);
    assert.match(await cb4.text(), /禁用/);
  } finally {
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
    for (const id of createdUserIds) await db.deleteUser(id).catch(() => {});
    await db.close();
    await idp.close();
  }
});

test("oidc routes 404 when not configured", async () => {
  const { server } = await createGatewayServer(testConfig(""), STATIC_FILE, undefined);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    const conf = await (await fetch(`http://localhost:${port}/auth/config`)).json() as { oidc: boolean };
    assert.equal(conf.oidc, false);
    assert.equal((await fetch(`http://localhost:${port}/auth/oidc/login`)).status, 404);
    assert.equal((await fetch(`http://localhost:${port}/auth/oidc/callback?code=x&state=y`)).status, 404);
  } finally {
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
  }
});
