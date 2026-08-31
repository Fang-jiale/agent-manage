// 工行 AAM 统一认证集成测试：mock Java 桥（node 脚本顶替）跑通
// 跳转 → ssiAuth/ssiSign 验签 → 按工号自动建号 → JWT 签发全链路。
// 桥协议与 package/aam/AamBridge.java 对齐：stdin 一行 JSON → stdout 一行 JSON。
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db } from "../src/db.ts";
import { verifyJwt } from "../src/auth.ts";
import { setLogLevel } from "../src/util.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const JWT_SECRET = "aam-test-secret";

// mock 桥：读一行 JSON，与真实桥（AamBridge.java）的输入输出协议一致，只是不跑 SM2：
// - 空参数（登录入口形态）→ 返回 SDK 风格的 redirect（AAM 授权页地址）
// - 正确的 ssiAuth/ssiSign → 返回工号（顺带断言 serviceName/serviceURL 透传）
// - 其余 → 报错
function writeMockBridge(): string {
  const file = path.join(os.tmpdir(), `aam-mock-bridge-${process.pid}.mjs`);
  fs.writeFileSync(file, `import readline from "node:readline";
const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  let msg = {};
  try { msg = JSON.parse(line); } catch {}
  if (!msg.ssiAuth && !msg.ssiSign) {
    // 登录入口：SDK 内部 sendRedirect 的地址
    const redirect = "https://aam.example/login?service=" + msg.serviceName + "&callback=" + encodeURIComponent(msg.serviceURL);
    process.stdout.write(JSON.stringify({ ok: false, error: "redirect", redirect }) + "\\n");
  } else if (msg.ssiAuth === "auth-ok" && msg.ssiSign === "sign-ok" && msg.serviceName === "test-svc") {
    process.stdout.write(JSON.stringify({
      ok: true,
      employeeNo: msg.serviceURL.includes("/auth/aam/callback") ? "100861234" : "",
      username: "zhangsan", name: "张三", department: "科技部",
    }) + "\\n");
  } else if (msg.ssiAuth === "auth-with-crlf") {
    // 验证网关侧已把 \\r\\n 剥掉
    process.stdout.write(JSON.stringify({ ok: msg.ssiAuth.includes("\\n") ? false : true, employeeNo: "100861235", username: "lisi", name: "李四" }) + "\\n");
  } else {
    process.stdout.write(JSON.stringify({ ok: false, error: "invalid ssiAuth/ssiSign" }) + "\\n");
  }
});
`);
  return file;
}

function testConfig(bridgeCmd: string): GatewayConfig {
  return {
    addr: ":0",
    logLevel: "error",
    agentTimeoutMs: 90_000,
    userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000,
    databaseURL: DB_URL,
    jwtSecret: JWT_SECRET,
    jwtTtlMs: 3_600_000,
    adminPassword: "x",
    redisURL: "",
    redisPrefix: "ywm",
    instanceID: "aam-test",
    attachDir: "",
    attachQuotaMb: 0,
    retentionDays: 0,
    s3Endpoint: "",
    s3Region: "us-east-1",
    s3Bucket: "ywmatrix",
    s3AccessKey: "",
    s3SecretKey: "",
    s3PublicURL: "",
    oidcIssuer: "",
    oidcClientID: "",
    oidcClientSecret: "",
    oidcRedirectURL: "",
    oidcEmployeeClaim: "employee_id",
    aamServer: "aam.example",
    aamServiceName: "test-svc",
    aamSmPublicKey: "{}",
    aamSmKeyPass: "pass",
    aamBridgeCmd: bridgeCmd,
    trustProxy: false,
  };
}

async function startGateway(cfg: GatewayConfig, db: Db): Promise<{ base: string; close: () => Promise<void> }> {
  const { server } = await createGatewayServer(cfg, STATIC_FILE, db);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    base: `http://localhost:${port}`,
    close: () => new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    }),
  };
}

test("aam sso full flow: login redirect, ssi verify, auto provision, jwt issue", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 AAM 测试");
    await db.close().catch(() => {});
    return;
  }
  const bridge = writeMockBridge();
  const gw = await startGateway(testConfig(`node ${bridge}`), db);
  try {
    // 未配置 AAM 的网关：不暴露入口，路由 404
    const gwOff = await startGateway(testConfig(""), db);
    const cfgOff = await (await fetch(`${gwOff.base}/auth/config`)).json() as { aam?: boolean };
    assert.equal(cfgOff.aam, false);
    assert.equal((await fetch(`${gwOff.base}/auth/aam/login`)).status, 404);
    await gwOff.close();

    // /auth/config 暴露 aam 入口
    assert.equal(((await (await fetch(`${gw.base}/auth/config`)).json()) as { aam?: boolean }).aam, true);

    // 登录入口：网关空参数调桥 → 302 到 SDK 返回的授权页地址（含 service 与回调）
    const loginResp = await fetch(`${gw.base}/auth/aam/login`, { redirect: "manual" });
    assert.equal(loginResp.status, 302);
    const location = loginResp.headers.get("location") ?? "";
    assert.ok(location.startsWith("https://aam.example/login?"), location);
    assert.ok(location.includes("service=test-svc"), location);
    assert.ok(location.includes(`callback=${encodeURIComponent(`http://localhost:${new URL(gw.base).port}/auth/aam/callback`)}`), location);

    // 回调：正确参数 → 桥验签 → 按工号首次自动建号 → 页面内嵌 JWT
    const cb = await fetch(`${gw.base}/auth/aam/callback?ssiAuth=auth-ok&ssiSign=sign-ok`);
    assert.equal(cb.status, 200);
    const html = await cb.text();
    const tokenMatch = /s\.token = "([^"]+)"/.exec(html);
    assert.ok(tokenMatch, "callback html embeds token");
    const claims = verifyJwt(tokenMatch![1], JWT_SECRET);
    assert.ok(claims, "token verifies against gateway secret");

    const user = await db.getUserByEmployeeID("100861234");
    assert.ok(user, "user provisioned by employeeNo");
    assert.equal(user?.id, claims?.sub);
    assert.equal(user?.display_name, "张三", "display_name 取 SSICUser.name");
    assert.equal(user?.role, "user");

    // 同工号再次登录：复用账号
    const cb2 = await fetch(`${gw.base}/auth/aam/callback?ssiAuth=auth-ok&ssiSign=sign-ok`);
    const token2 = /s\.token = "([^"]+)"/.exec(await cb2.text())![1];
    assert.equal(verifyJwt(token2, JWT_SECRET)?.sub, user?.id, "same user reused");

    // 带 \r\n 的入参：网关侧剥离后桥验签通过（文档要求，防签名校验失败）
    const cb3 = await fetch(`${gw.base}/auth/aam/callback?ssiAuth=${encodeURIComponent("auth-with-crlf\r\n")}&ssiSign=sign-ok`);
    assert.equal(cb3.status, 200);

    // 错误参数 → 失败页
    const bad = await fetch(`${gw.base}/auth/aam/callback?ssiAuth=nope&ssiSign=nope`);
    assert.equal(bad.status, 400);
    assert.ok((await bad.text()).includes("invalid"));

    // 缺参数 → 400
    assert.equal((await fetch(`${gw.base}/auth/aam/callback`)).status, 400);

    await db.deleteUser(user!.id).catch(() => {});
  } finally {
    await gw.close();
    fs.rmSync(bridge, { force: true });
    await db.close();
  }
});

// 桥命令本身坏掉（java 不存在/类缺失）时：明确报错而非 500 假死
test("aam bridge failure surfaces as login failure", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 AAM 测试");
    await db.close().catch(() => {});
    return;
  }
  const gw = await startGateway(testConfig("/nonexistent/aam-bridge-binary"), db);
  try {
    const resp = await fetch(`${gw.base}/auth/aam/callback?ssiAuth=a&ssiSign=b`);
    assert.equal(resp.status, 400);
    assert.ok((await resp.text()).includes("登录失败"));
  } finally {
    await gw.close();
    await db.close();
  }
});
