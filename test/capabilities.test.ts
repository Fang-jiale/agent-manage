import { test } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db, type DbAgentBrand } from "../src/db.ts";
import { setLogLevel } from "../src/util.ts";
import { signJwt } from "../src/auth.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "cap-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-cap-test";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "cap-test",
    attachDir: "", attachQuotaMb: 0, retentionDays: 0,
    s3Endpoint: "", s3Region: "us-east-1", s3Bucket: "ywmatrix",
    s3AccessKey: "", s3SecretKey: "", s3PublicURL: "",
    oidcIssuer: "", oidcClientID: "", oidcClientSecret: "", oidcRedirectURL: "",
    oidcEmployeeClaim: "employee_id",
    trustProxy: false,
  };
}

class Conn {
  private ws: WebSocket;
  private buffer: proto.Message[] = [];
  private waiters: { method?: string; resolve: (m: proto.Message) => void; timer: NodeJS.Timeout }[] = [];

  private constructor(ws: WebSocket) {
    this.ws = ws;
    ws.on("message", (data) => {
      const msg = JSON.parse(data.toString()) as proto.Message;
      const idx = this.waiters.findIndex((w) => w.method === undefined || w.method === msg.method);
      if (idx !== -1) {
        const [w] = this.waiters.splice(idx, 1);
        clearTimeout(w.timer);
        w.resolve(msg);
      } else {
        this.buffer.push(msg);
      }
    });
  }

  static dial(url: string): Promise<Conn> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.once("open", () => resolve(new Conn(ws)));
      ws.once("error", reject);
    });
  }

  send(msg: proto.Message): void {
    this.ws.send(JSON.stringify(msg));
  }

  next(method?: string, timeoutMs = 5000): Promise<proto.Message> {
    const idx = this.buffer.findIndex((m) => method === undefined || m.method === method);
    if (idx !== -1) {
      const [m] = this.buffer.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.waiters.findIndex((w) => w.resolve === wrapped);
        if (i !== -1) this.waiters.splice(i, 1);
        reject(new Error(`timeout waiting for ${method ?? "message"}`));
      }, timeoutMs);
      const wrapped = (m: proto.Message): void => resolve(m);
      this.waiters.push({ method, resolve: wrapped, timer });
    });
  }

  close(): void {
    this.ws.close();
  }
}

async function registerAgent(conn: Conn, agentID: string): Promise<void> {
  conn.send(proto.newRequest(`reg-${agentID}`, proto.METHOD_REGISTER, {
    agent_id: agentID,
    name: agentID,
    capabilities: [{ type: "chat", name: "general" }],
  } satisfies proto.RegisterParams));
  const resp = await conn.next();
  assert.equal(resp.error, undefined, `register error: ${JSON.stringify(resp.error)}`);
}

interface Fixture {
  base: string;
  db: Db;
  close: () => Promise<void>;
}

async function startFixture(t: import("node:test").TestContext): Promise<Fixture | undefined> {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 capabilities 测试");
    await db.close().catch(() => {});
    return undefined;
  }
  const savedBrands: DbAgentBrand[] = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});
  if (!(await db.getUserById(OWNER))) {
    await db.createUser({ id: OWNER, name: OWNER, password_hash: "x" });
  }
  const { server } = await createGatewayServer(testConfig(), STATIC_FILE, db, undefined);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    base: `ws://localhost:${port}`,
    db,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      for (const b of savedBrands) {
        await db.createBrand({
          id: b.id, name: b.name, description: b.description, logo_url: b.logo_url,
          capabilities: b.capabilities, launch_cmd: b.launch_cmd, conn_type: b.conn_type, endpoint: b.endpoint,
        }).catch(() => {});
      }
      await db.close();
    },
  };
}

// C1 两级作用域：带 session_id 的更新不改 Agent 全局能力、以含 session_id 的通知推给页面；
// 不带 session_id 的全局更新走 admin.agentList 广播刷新全局能力。
test("capabilities_updated two-level scope (session vs global)", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { base } = fx;
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "cap-a1");

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 会话级快照：页面直接收到含 session_id 的通知
    agentConn.send(proto.newNotification(proto.METHOD_CAPABILITIES_UPDATED, {
      agent_id: "cap-a1", session_id: "sess-cap-1",
      capabilities: [{ type: "command", name: "deploy", description: "项目内部署" }],
    } satisfies proto.CapabilitiesUpdatedParams));
    const scoped = proto.decodeParams<proto.CapabilitiesUpdatedParams>(
      await userConn.next(proto.METHOD_CAPABILITIES_UPDATED));
    assert.equal(scoped.agent_id, "cap-a1");
    assert.equal(scoped.session_id, "sess-cap-1");
    assert.equal(scoped.capabilities[0]?.name, "deploy");

    // 全局更新：不推 system.capabilities_updated，走 admin.agentList（1s 合并窗口），
    // 且全局能力不包含会话层的 deploy（分层全量替换）
    agentConn.send(proto.newNotification(proto.METHOD_CAPABILITIES_UPDATED, {
      agent_id: "cap-a1",
      capabilities: [{ type: "command", name: "build", description: "全局构建" }],
    } satisfies proto.CapabilitiesUpdatedParams));
    const list = await userConn.next(proto.METHOD_ADMIN_AGENT_LIST, 10_000);
    const params = proto.decodeParams<proto.AgentListParams>(list);
    const a = (params.agents || []).find((x) => x.id === "cap-a1");
    assert.ok(a, "agent in list");
    const names = (a?.capabilities || []).map((c) => c.name);
    assert.ok(names.includes("build"), `global has build: ${names}`);
    assert.ok(!names.includes("deploy"), `session layer not leaked to global: ${names}`);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});
