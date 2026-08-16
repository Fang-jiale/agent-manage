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
const JWT_SECRET = "perm-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-perm-test";
const OTHER = "perm-other";
const ADMIN = "perm-admin";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "perm-test",
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

interface Fixture {
  base: string;
  db: Db;
  close: () => Promise<void>;
}

// 起真实网关 + MySQL；品牌目录快照后清空（开放模式），结束后恢复
async function startFixture(t: import("node:test").TestContext): Promise<Fixture | undefined> {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过权限测试");
    await db.close().catch(() => {});
    return undefined;
  }
  const savedBrands: DbAgentBrand[] = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});
  for (const [id] of [[OWNER], [OTHER], [ADMIN]] as const) {
    if (!(await db.getUserById(id))) {
      await db.createUser({ id, name: id, password_hash: "x" });
    }
  }
  await db.setUserRole(ADMIN, "admin");
  // 清理上次残留
  for (const [id, owner] of [["perm-a1", OWNER], ["perm-a2", OWNER], ["perm-b1", OTHER]] as const) {
    await db.unassignAgent(id).catch(() => {});
    await db.setNickname(owner, id, null).catch(() => {});
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

async function upsertAgentRow(db: Db, agentID: string, ownerID: string): Promise<void> {
  await db.upsertAgent({
    id: agentID, owner_id: ownerID, name: agentID,
    platform: null, capabilities: JSON.stringify([{ type: "chat", name: "general" }]), status: "online",
  });
}

test("agent.set_nickname owner enforcement and push", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  try {
    await upsertAgentRow(db, "perm-a1", OWNER);
    // 推送只含在线 agent：注册一个真实连接
    const agentConn = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(agentConn);
    agentConn.send(proto.newRequest("reg-perm", proto.METHOD_REGISTER, {
      agent_id: "perm-a1", name: "perm-a1", capabilities: [{ type: "chat", name: "general" }],
    } satisfies proto.RegisterParams));
    assert.equal((await agentConn.next()).error, undefined);

    const ownerConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    const otherConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OTHER)}`);
    conns.push(ownerConn, otherConn);
    await ownerConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    await otherConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 他人 agent → INVALID_PARAMS
    otherConn.send(proto.newRequest("nk-0", proto.METHOD_AGENT_SET_NICKNAME, {
      agent_id: "perm-a1", nickname: "抢注",
    } satisfies proto.AgentSetNicknameParams));
    assert.equal((await otherConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    // 属主设置 → 下一帧推送带 nickname
    ownerConn.send(proto.newRequest("nk-1", proto.METHOD_AGENT_SET_NICKNAME, {
      agent_id: "perm-a1", nickname: "我的机器",
    } satisfies proto.AgentSetNicknameParams));
    const resp = await ownerConn.next();
    assert.equal(resp.error, undefined, JSON.stringify(resp.error));
    assert.equal((resp.result as proto.AgentSetNicknameResult).nickname, "我的机器");
    const push = proto.decodeParams<proto.AgentListParams>(await ownerConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    const a = push.agents.find((x) => x.id === "perm-a1");
    assert.ok(a, "push contains perm-a1");
    assert.equal(a.nickname, "我的机器");

    // 空昵称 → 清除，推送回退 null
    ownerConn.send(proto.newRequest("nk-2", proto.METHOD_AGENT_SET_NICKNAME, {
      agent_id: "perm-a1", nickname: "",
    } satisfies proto.AgentSetNicknameParams));
    await ownerConn.next();
    const push2 = proto.decodeParams<proto.AgentListParams>(await ownerConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(push2.agents.find((x) => x.id === "perm-a1")?.nickname, null);

    await db.unassignAgent("perm-a1").catch(() => {});
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("agent.list scoped for normal users, full for admin", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  try {
    await upsertAgentRow(db, "perm-a1", OWNER);
    await upsertAgentRow(db, "perm-b1", OTHER);
    await db.setNickname(OWNER, "perm-a1", "备注A");

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OTHER)}`);
    const adminConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(ADMIN)}`);
    conns.push(userConn, adminConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 普通用户：只见自己；显式传他人 owner_id 也被服务端覆盖
    for (const reqID of ["al-1", "al-2"]) {
      userConn.send(proto.newRequest(reqID, proto.METHOD_AGENT_LIST, reqID === "al-2" ? { owner_id: OWNER } : {}));
      const listed = await userConn.next();
      assert.equal(listed.error, undefined, JSON.stringify(listed.error));
      const agents = (listed.result as proto.AdminAgentListResult).agents;
      assert.ok(agents.every((a) => a.owner_id === OTHER), "normal user sees only own agents");
      assert.ok(agents.some((a) => a.id === "perm-b1"));
      assert.ok(!agents.some((a) => a.id === "perm-a1"));
    }

    // admin：全量
    adminConn.send(proto.newRequest("al-3", proto.METHOD_AGENT_LIST, {}));
    const adminListed = await adminConn.next();
    assert.equal(adminListed.error, undefined, JSON.stringify(adminListed.error));
    const all = (adminListed.result as proto.AdminAgentListResult).agents;
    assert.ok(all.some((a) => a.id === "perm-a1") && all.some((a) => a.id === "perm-b1"));

    await db.unassignAgent("perm-a1").catch(() => {});
    await db.unassignAgent("perm-b1").catch(() => {});
    await db.setNickname(OWNER, "perm-a1", null).catch(() => {});
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("brand.list readable by normal user, writes and approvals admin-only", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { base } = fx;
  const conns: Conn[] = [];
  try {
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OTHER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // brand.list 只读开放
    userConn.send(proto.newRequest("bl-1", proto.METHOD_BRAND_LIST, {}));
    const listed = await userConn.next();
    assert.equal(listed.error, undefined, JSON.stringify(listed.error));
    assert.ok(Array.isArray((listed.result as proto.BrandListResult).brands));

    // brand.create 仍 admin-only
    userConn.send(proto.newRequest("bc-1", proto.METHOD_BRAND_CREATE, {
      name: `perm-brand-${Date.now()}`, capabilities: [],
    } satisfies proto.BrandCreateParams));
    assert.equal((await userConn.next()).error?.code, proto.ERR_UNAUTHORIZED);

    // 审批类仍 admin-only
    userConn.send(proto.newRequest("ap-1", proto.METHOD_AGENT_APPROVE, { agent_id: "perm-a1" } satisfies proto.AgentApprovalParams));
    assert.equal((await userConn.next()).error?.code, proto.ERR_UNAUTHORIZED);
    userConn.send(proto.newRequest("ov-1", proto.METHOD_ADMIN_OVERVIEW, {}));
    assert.equal((await userConn.next()).error?.code, proto.ERR_UNAUTHORIZED);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});
