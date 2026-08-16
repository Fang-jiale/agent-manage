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
const JWT_SECRET = "interaction-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-interaction-test";
const AGENT_ID = "it-agent";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "interaction-test",
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

async function rpc(conn: Conn, id: string, method: string, params: object): Promise<proto.Message> {
  conn.send(proto.newRequest(id, method, params));
  for (;;) {
    const m = await conn.next();
    if (m.id === id) return m;
  }
}

// 交互卡片应答状态持久化：task.respond 把缓冲中的 confirm chunk 标记 answered+answer，
// 任务结束时随消息落库——前端刷新后从历史加载不再回退成待确认
test("interaction answer and cancel persist with task message", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过交互持久化测试");
    await db.close().catch(() => {});
    return;
  }
  const savedBrands: DbAgentBrand[] = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});
  if (!(await db.getUserById(OWNER))) {
    await db.createUser({ id: OWNER, name: OWNER, password_hash: "x" });
  }
  const { server } = await createGatewayServer(testConfig(), STATIC_FILE, db, undefined);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const base = `ws://localhost:${(server.address() as AddressInfo).port}`;
  const conns: Conn[] = [];
  const sessionID = crypto.randomUUID();

  try {
    const agentConn = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(agentConn);
    agentConn.send(proto.newRequest("reg-1", proto.METHOD_REGISTER, {
      agent_id: AGENT_ID, name: AGENT_ID,
      capabilities: [{ type: "chat", name: "general" }],
    } satisfies proto.RegisterParams));
    assert.equal((await agentConn.next()).error, undefined);
    await db.upsertAgent({
      id: AGENT_ID, owner_id: OWNER, name: AGENT_ID,
      platform: null, capabilities: JSON.stringify([{ type: "chat", name: "general" }]), status: "online",
    });

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    await rpc(userConn, "sc-1", proto.METHOD_SESSION_CREATE, {
      agent_id: AGENT_ID, id: sessionID,
    } satisfies proto.SessionCreateParams);

    // 任务 1：confirm_required → 用户 task.respond(allow) → END；落库 chunk 应带 answered+answer
    const chat1P = (async () => {
      const msg = await agentConn.next(proto.METHOD_AGENT_CHAT);
      agentConn.send(proto.newResponse(msg.id ?? "", { status: "accepted", task_id: msg.id ?? "" } satisfies proto.TaskAcceptResult));
      return proto.decodeParams<proto.AgentChatParams>(msg);
    })();
    await rpc(userConn, "t-1", proto.METHOD_TASK_CREATE, {
      agent_id: AGENT_ID, session_id: sessionID, task_id: "it-t1", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    const chat1 = await chat1P;

    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: chat1.task_id,
      value: {
        kind: proto.PROGRESS_KIND_REPORT, type: proto.CHUNK_TYPE_CONFIRM_REQUIRED,
        agent_id: AGENT_ID, task_id: chat1.task_id, confirm_id: "cf-1",
        content: proto.textContent("要继续吗"),
      },
    } satisfies proto.ProgressParams));
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);
    // task.respond 的响应由 agent 对 agent.respond 的应答回投路由，须并发补答
    const respondAckP = (async () => {
      const msg = await agentConn.next(proto.METHOD_AGENT_RESPOND);
      agentConn.send(proto.newResponse(msg.id ?? "", { status: "ok" }));
    })();
    const resp = await rpc(userConn, "r-1", proto.METHOD_TASK_RESPOND, {
      agent_id: AGENT_ID, task_id: chat1.task_id, confirm_id: "cf-1",
      response: { decision: proto.RESPOND_DECISION_ALLOW },
    } satisfies proto.TaskRespondParams);
    assert.equal(resp.error, undefined);
    await respondAckP;
    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: chat1.task_id,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: AGENT_ID, task_id: chat1.task_id, content: proto.textContent("done"), done: true,
      },
    } satisfies proto.ProgressParams));
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);

    // 任务 2：confirm_required 未应答直接 END → chunk 应带 cancelled
    const chat2P = (async () => {
      const msg = await agentConn.next(proto.METHOD_AGENT_CHAT);
      agentConn.send(proto.newResponse(msg.id ?? "", { status: "accepted", task_id: msg.id ?? "" } satisfies proto.TaskAcceptResult));
      return proto.decodeParams<proto.AgentChatParams>(msg);
    })();
    await rpc(userConn, "t-2", proto.METHOD_TASK_CREATE, {
      agent_id: AGENT_ID, session_id: sessionID, task_id: "it-t2", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    const chat2 = await chat2P;
    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: chat2.task_id,
      value: {
        kind: proto.PROGRESS_KIND_REPORT, type: proto.CHUNK_TYPE_CONFIRM_REQUIRED,
        agent_id: AGENT_ID, task_id: chat2.task_id, confirm_id: "cf-2",
        content: proto.textContent("没人理我"),
      },
    } satisfies proto.ProgressParams));
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);
    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: chat2.task_id,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: AGENT_ID, task_id: chat2.task_id, content: proto.textContent("bye"), done: true,
      },
    } satisfies proto.ProgressParams));
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);

    // 等落库后校验 chunk 标记
    const deadline = Date.now() + 5000;
    let msgs: Array<{ role: string; content: string; task_id?: string | null }> = [];
    while (Date.now() < deadline) {
      msgs = await db.listMessages(OWNER, sessionID, 50);
      if (msgs.filter((m) => m.role === "assistant").length >= 2) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    const byTask = new Map(msgs.filter((m) => m.role === "assistant")
      .map((m) => [m.task_id, JSON.parse(m.content).chunks as Array<Record<string, unknown>>]));
    const t1Chunks = byTask.get("it-t1") ?? [];
    const cf1 = t1Chunks.find((c) => c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED);
    assert.ok(cf1, "task1 应包含 confirm chunk");
    assert.equal(cf1.answered, true);
    assert.deepEqual(cf1.answer, { decision: proto.RESPOND_DECISION_ALLOW });

    const t2Chunks = byTask.get("it-t2") ?? [];
    const cf2 = t2Chunks.find((c) => c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED);
    assert.ok(cf2, "task2 应包含 confirm chunk");
    assert.equal(cf2.cancelled, true);
    assert.equal(cf2.answered, undefined);
  } finally {
    for (const c of conns) c.close();
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
    await db.deleteSession(OWNER, sessionID).catch(() => {});
    await db.deleteUser(OWNER).catch(() => {});
    for (const b of savedBrands) {
      await db.createBrand({
        id: b.id, name: b.name, description: b.description, logo_url: b.logo_url,
        capabilities: b.capabilities, launch_cmd: b.launch_cmd, conn_type: b.conn_type, endpoint: b.endpoint,
      }).catch(() => {});
    }
    await db.close();
  }
});
