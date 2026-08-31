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
  for (const [id, owner] of [
    ["perm-a1", OWNER], ["perm-a2", OWNER], ["perm-b1", OTHER],
    ["perm-hijack-1", OWNER], ["perm-prog-a", OWNER], ["perm-prog-b", OTHER],
    ["perm-ffa-1", OWNER],
  ] as const) {
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

// 归属校验：他人持自己的有效凭据抢注已有 agent_id，必须被拒且不踢正主连接、不改归属
test("agent.register cannot hijack another user's agent_id", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  await upsertAgentRow(db, "perm-hijack-1", OWNER);
  const owner = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
  try {
    owner.send(proto.newRequest("reg-ok", proto.METHOD_REGISTER, {
      agent_id: "perm-hijack-1", name: "perm-hijack-1",
      capabilities: [{ type: "chat", name: "general" }],
    } satisfies proto.RegisterParams));
    const ok = await owner.next();
    assert.equal(ok.error, undefined, JSON.stringify(ok.error));

    const attacker = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OTHER)}`);
    try {
      attacker.send(proto.newRequest("reg-bad", proto.METHOD_REGISTER, {
        agent_id: "perm-hijack-1", name: "stolen", capabilities: [],
      } satisfies proto.RegisterParams));
      const denied = await attacker.next();
      assert.equal(denied.error?.code, proto.ERR_UNAUTHORIZED, JSON.stringify(denied.error));

      // 正主连接未被踢（4001/4002 都会关 ws）：心跳仍能得到响应，归属未变
      owner.send(proto.newRequest("hb-1", proto.METHOD_HEARTBEAT, {
        agent_id: "perm-hijack-1", timestamp: new Date().toISOString(),
      } satisfies proto.HeartbeatParams));
      const hb = await owner.next();
      assert.equal(hb.error, undefined, JSON.stringify(hb.error));
      const row = await db.getAgentRow("perm-hijack-1");
      assert.equal(row?.owner_id, OWNER);
    } finally {
      attacker.close();
    }
  } finally {
    owner.close();
    await fx.close();
  }
});

// product.push 是管理动作：非 admin 拒绝；admin 也只能推目录里真实存在的包
test("product.push is admin-only and catalog-checked", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { base } = fx;
  const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OTHER)}`);
  const adminConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(ADMIN)}`);
  try {
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("push-1", proto.METHOD_PRODUCT_PUSH, {
      brand: "no-such-brand", version: "9.9.9",
    }));
    const denied = await userConn.next();
    assert.equal(denied.error?.code, proto.ERR_UNAUTHORIZED, JSON.stringify(denied.error));

    adminConn.send(proto.newRequest("push-2", proto.METHOD_PRODUCT_PUSH, {
      brand: "no-such-brand", version: "9.9.9",
    }));
    const missing = await adminConn.next();
    assert.equal(missing.error?.code, proto.ERR_INVALID_PARAMS, JSON.stringify(missing.error));
  } finally {
    userConn.close();
    adminConn.close();
    await fx.close();
  }
});

// 连接鉴权 ≠ 内容鉴权：payload 里的 agent_id / task_id 不属于本连接托管时，进度必须被忽略
test("progress with foreign agent_id or task_id is ignored", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  try {
    await upsertAgentRow(db, "perm-prog-a", OWNER);
    const agentA = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const agentB = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OTHER)}`);
    conns.push(agentA, agentB);
    for (const [conn, id] of [[agentA, "perm-prog-a"], [agentB, "perm-prog-b"]] as const) {
      conn.send(proto.newRequest("reg-" + id, proto.METHOD_REGISTER, {
        agent_id: id, name: id, capabilities: [{ type: "chat", name: "general" }],
      } satisfies proto.RegisterParams));
      const r = await conn.next();
      assert.equal(r.error, undefined, JSON.stringify(r.error));
    }
    // 等注册广播的 1s 防抖落定，避免后续 next() 抓到 agent.list 推送
    await new Promise((r) => setTimeout(r, 1300));

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const taskID = "perm-task-1";
    userConn.send(proto.newRequest("t-1", proto.METHOD_TASK_CREATE, {
      agent_id: "perm-prog-a", task_id: taskID, type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams));
    const chat = await agentA.next(proto.METHOD_AGENT_CHAT);
    assert.equal(chat.id, "t-1");
    agentA.send(proto.newResponse("t-1", { status: "accepted", task_id: taskID } satisfies proto.TaskAcceptResult));
    const accept = await userConn.next();
    assert.equal(accept.id, "t-1");

    // B 用自己的 agent_id 配 A 的 task_id：任务不属于 B → 忽略
    agentB.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: taskID,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: "perm-prog-b", task_id: taskID,
        content: proto.textContent("forged-by-b"), done: true,
      },
    } satisfies proto.ProgressParams));
    // B 直接伪造 A 的 agent_id：非本连接托管 → 忽略
    agentB.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: taskID,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: "perm-prog-a", task_id: taskID,
        content: proto.textContent("forged-as-a"), done: true,
      },
    } satisfies proto.ProgressParams));
    // B 谎报 A 下线 → 忽略，A 仍在线
    agentB.send(proto.newRequest("st-1", proto.METHOD_STATUS, {
      agent_id: "perm-prog-a", status: proto.AGENT_STATUS_OFFLINE,
    } satisfies proto.StatusParams));

    const leaked = await userConn.next(proto.METHOD_ADMIN_PROGRESS, 500).then(
      () => "leaked", () => "none");
    assert.equal(leaked, "none", "forged progress must not reach the task owner");

    // 正主 A 的 done 正常送达，任务正常收尾
    const chatParams = proto.decodeParams<proto.AgentChatParams>(chat);
    agentA.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: taskID,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: "perm-prog-a", task_id: taskID,
        session_id: chatParams.session_id,
        content: proto.textContent("real done"), done: true,
      },
    } satisfies proto.ProgressParams));
    const progress = proto.decodeParams<proto.AdminProgressParams>(await userConn.next(proto.METHOD_ADMIN_PROGRESS));
    assert.equal(progress.done, true);

    userConn.send(proto.newRequest("al-9", proto.METHOD_AGENT_LIST, {}));
    const listed = await userConn.next();
    assert.equal(listed.error, undefined, JSON.stringify(listed.error));
    const agents = (listed.result as proto.AdminAgentListResult).agents;
    assert.equal(agents.find((a) => a.id === "perm-prog-a")?.status, proto.AGENT_STATUS_ONLINE,
      "spoofed offline status must not unregister the victim agent");

    await db.deleteSession(OWNER, chatParams.session_id || `${taskID}-session`).catch(() => {});
    await db.unassignAgent("perm-prog-a").catch(() => {});
    await db.unassignAgent("perm-prog-b").catch(() => {});
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

// 首帧认证：凭证走第一条 auth 消息而非 URL query（反代日志/浏览器历史不再记录 token）
test("first-frame auth: credentials via auth message, not URL", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conn = await Conn.dial(`${base}/ws/agent`); // 无 query 凭证
  try {
    conn.send(proto.newRequest("auth-1", proto.METHOD_AUTH, { token: jwtFor(OWNER) } satisfies proto.AuthParams));
    const authResp = await conn.next();
    assert.equal(authResp.error, undefined, JSON.stringify(authResp.error));
    assert.equal((authResp.result as { status?: string }).status, "ok");

    // 认证通过后正常注册（身份 = 首帧 token 的属主）
    conn.send(proto.newRequest("reg-1", proto.METHOD_REGISTER, {
      agent_id: "perm-ffa-1", name: "perm-ffa-1", capabilities: [{ type: "chat", name: "general" }],
    } satisfies proto.RegisterParams));
    const reg = await conn.next();
    assert.equal(reg.error, undefined, JSON.stringify(reg.error));

    // 伪造 token 首帧 → 拒绝并断开
    const bad = await Conn.dial(`${base}/ws/agent`);
    try {
      bad.send(proto.newRequest("auth-1", proto.METHOD_AUTH, { token: "forged.jwt.sig" } satisfies proto.AuthParams));
      const denied = await bad.next();
      assert.equal(denied.error?.code, proto.ERR_UNAUTHORIZED, JSON.stringify(denied.error));
    } finally {
      bad.close();
    }

    // 首帧不是 auth → 拒绝并断开
    const bad2 = await Conn.dial(`${base}/ws/agent`);
    try {
      bad2.send(proto.newRequest("reg-x", proto.METHOD_REGISTER, {
        agent_id: "perm-ffa-2", name: "x", capabilities: [],
      } satisfies proto.RegisterParams));
      const denied2 = await bad2.next();
      assert.equal(denied2.error?.code, proto.ERR_UNAUTHORIZED, JSON.stringify(denied2.error));
    } finally {
      bad2.close();
    }

    await db.unassignAgent("perm-ffa-1").catch(() => {});
  } finally {
    conn.close();
    await fx.close();
  }
});
