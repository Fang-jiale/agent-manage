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
const JWT_SECRET = "workdir-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-workdir-test";
const AGENT_ID = "wd-agent";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "workdir-test",
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
  // 丢弃插队的广播通知（agentList 有 1s 合并窗口），只认 id 匹配的响应
  for (;;) {
    const m = await conn.next();
    if (m.id === id) return m;
  }
}

// 收 agent.chat 并回 accepted（单 agent 路径的 task.create 响应由 agent 的 ack 驱动）
async function nextChat(conn: Conn): Promise<proto.AgentChatParams> {
  const msg = await conn.next(proto.METHOD_AGENT_CHAT);
  conn.send(proto.newResponse(msg.id ?? "", { status: "accepted", task_id: msg.id ?? "" } satisfies proto.TaskAcceptResult));
  return proto.decodeParams<proto.AgentChatParams>(msg);
}

// 会话工作目录：session.create/set_workdir 存取 + task.create 注入 metadata.workdir
test("session workdir bind and metadata injection", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 workdir 测试");
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
      agent_id: AGENT_ID,
      name: AGENT_ID,
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

    // create 带 workdir；返回值与 list 都带
    const created = await rpc(userConn, "sc-1", proto.METHOD_SESSION_CREATE, {
      agent_id: AGENT_ID, id: sessionID, workdir: "/tmp/proj-a",
    } satisfies proto.SessionCreateParams);
    assert.equal(created.error, undefined, JSON.stringify(created.error));
    assert.equal((created.result as proto.SessionInfo).workdir, "/tmp/proj-a");

    const listed = await rpc(userConn, "sl-1", proto.METHOD_SESSION_LIST, {} satisfies proto.SessionListParams);
    const row = ((listed.result as proto.SessionListResult).sessions).find((s) => s.id === sessionID);
    assert.equal(row?.workdir, "/tmp/proj-a");

    // task.create → agent.chat 注入 metadata.workdir（响应由 agent ack 驱动，须并发收 chat）
    const chat1P = nextChat(agentConn);
    await rpc(userConn, "t-1", proto.METHOD_TASK_CREATE, {
      agent_id: AGENT_ID, session_id: sessionID, task_id: "wd-t1", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    const chat1 = await chat1P;
    assert.equal(chat1.metadata?.workdir, "/tmp/proj-a");

    // set_workdir 换目录 → 新值注入
    const set = await rpc(userConn, "sw-1", proto.METHOD_SESSION_SET_WORKDIR, {
      id: sessionID, workdir: "/tmp/proj-b",
    } satisfies proto.SessionSetWorkdirParams);
    assert.equal(set.error, undefined);
    assert.equal((set.result as { workdir: string }).workdir, "/tmp/proj-b");
    const chat2P = nextChat(agentConn);
    await rpc(userConn, "t-2", proto.METHOD_TASK_CREATE, {
      agent_id: AGENT_ID, session_id: sessionID, task_id: "wd-t2", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    const chat2 = await chat2P;
    assert.equal(chat2.metadata?.workdir, "/tmp/proj-b");

    // 清除绑定 → metadata 不再带 workdir
    await rpc(userConn, "sw-2", proto.METHOD_SESSION_SET_WORKDIR, {
      id: sessionID, workdir: "",
    } satisfies proto.SessionSetWorkdirParams);
    const chat3P = nextChat(agentConn);
    await rpc(userConn, "t-3", proto.METHOD_TASK_CREATE, {
      agent_id: AGENT_ID, session_id: sessionID, task_id: "wd-t3", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    const chat3 = await chat3P;
    assert.equal(chat3.metadata?.workdir, undefined);

    // 不存在的会话 → INVALID_PARAMS
    const miss = await rpc(userConn, "sw-3", proto.METHOD_SESSION_SET_WORKDIR, {
      id: "no-such-session", workdir: "/x",
    } satisfies proto.SessionSetWorkdirParams);
    assert.equal(miss.error?.code, proto.ERR_INVALID_PARAMS);

    // 超 512 字符 → INVALID_PARAMS（列宽保护）
    const tooLong = await rpc(userConn, "sw-4", proto.METHOD_SESSION_SET_WORKDIR, {
      id: sessionID, workdir: "/" + "a".repeat(600),
    } satisfies proto.SessionSetWorkdirParams);
    assert.equal(tooLong.error?.code, proto.ERR_INVALID_PARAMS);
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
