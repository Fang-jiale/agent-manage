import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, Hub, type GatewayConfig } from "../src/gateway.ts";
import { createLocalAttachmentStore } from "../src/storage.ts";
import { setLogLevel } from "../src/util.ts";
import { signJwt } from "../src/auth.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "test-secret";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    addr: ":0",
    logLevel: "error",
    agentTimeoutMs: 90_000,
    userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000,
    databaseURL: "",
    jwtSecret: JWT_SECRET,
    jwtTtlMs: 3_600_000,
    adminPassword: "x",
    redisURL: "",
    redisPrefix: "ywm",
    instanceID: "test-instance",
    attachDir: "",
    attachQuotaMb: 0,
    retentionDays: 0,
    s3Endpoint: "",
    s3Region: "us-east-1",
    s3Bucket: "ywmatrix",
    s3AccessKey: "",
    s3SecretKey: "",
    s3PublicURL: "",
    oidcIssuer: "", oidcClientID: "", oidcClientSecret: "", oidcRedirectURL: "",
    oidcEmployeeClaim: "employee_id",
    trustProxy: false,
    ...overrides,
  };
}

interface TestServer {
  base: string;
  close: () => Promise<void>;
}

async function startGateway(cfg: GatewayConfig, attachments?: import("../src/storage.ts").AttachmentStore): Promise<TestServer> {
  const { server } = await createGatewayServer(cfg, STATIC_FILE, undefined, attachments);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    base: `ws://localhost:${port}`,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

// Conn buffers incoming messages from the moment the socket opens so that
// early server pushes are never lost.
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
  conn.send(proto.newRequest("reg-1", proto.METHOD_REGISTER, {
    agent_id: agentID,
    name: agentID,
    capabilities: [{ type: "chat", name: "general" }],
  } satisfies proto.RegisterParams));
  const resp = await conn.next();
  assert.equal(resp.error, undefined, `register error: ${JSON.stringify(resp.error)}`);
  assert.equal(resp.id, "reg-1");
}

test("register and list", async () => {
  const srv = await startGateway(testConfig());
  const conns: Conn[] = [];
  try {
    const userConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("tester")}`);
    conns.push(userConn);
    let params = proto.decodeParams<proto.AgentListParams>(await userConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(params.agents.length, 0);

    const agentConn = await Conn.dial(`${srv.base}/ws/agent?token=${jwtFor("tester")}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-1");

    params = proto.decodeParams<proto.AgentListParams>(await userConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(params.agents.length, 1);
    assert.equal(params.agents[0].id, "agent-1");
  } finally {
    for (const c of conns) c.close();
    await srv.close();
  }
});

test("owner isolation", async () => {
  const srv = await startGateway(testConfig());
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${srv.base}/ws/agent?token=${jwtFor("tester")}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-1");

    // Another user should not see the agent.
    const otherConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("other")}`);
    conns.push(otherConn);
    let params = proto.decodeParams<proto.AgentListParams>(await otherConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(params.agents.length, 0);

    // The owner should see the agent.
    const ownerConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("tester")}`);
    conns.push(ownerConn);
    params = proto.decodeParams<proto.AgentListParams>(await ownerConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(params.agents.length, 1);
  } finally {
    for (const c of conns) c.close();
    await srv.close();
  }
});

test("agent removed on disconnect", async () => {
  const srv = await startGateway(testConfig({ agentTimeoutMs: 200 }));
  const conns: Conn[] = [];
  try {
    const userConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("tester")}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const agentConn = await Conn.dial(`${srv.base}/ws/agent?token=${jwtFor("tester")}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-1");
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    agentConn.close();

    // The hub should remove the agent; the next list broadcast should be empty.
    const params = proto.decodeParams<proto.AgentListParams>(await userConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(params.agents.length, 0);
  } finally {
    for (const c of conns) c.close();
    await srv.close();
  }
});

test("task.create routes to agent and response returns to user", async () => {
  const srv = await startGateway(testConfig());
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${srv.base}/ws/agent?token=${jwtFor("tester")}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-1");

    const userConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("tester")}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("req-1", proto.METHOD_TASK_CREATE, {
      agent_id: "agent-1",
      task_id: "task-1",
      type: "chat",
      content: "hello",
    } satisfies proto.TaskCreateParams));

    // Agent receives agent.chat with the same request id.
    const chatReq = await agentConn.next(proto.METHOD_AGENT_CHAT);
    assert.equal(chatReq.id, "req-1");
    const chatParams = proto.decodeParams<proto.AgentChatParams>(chatReq);
    assert.equal(chatParams.task_id, "task-1");
    assert.equal(chatParams.content, "hello");

    // Agent accepts; response should be routed back to the pending user request.
    agentConn.send(proto.newResponse("req-1", {
      status: "accepted",
      task_id: "task-1",
    } satisfies proto.TaskAcceptResult));

    const acceptResp = await userConn.next();
    assert.equal(acceptResp.id, "req-1");
    const result = acceptResp.result as proto.TaskAcceptResult;
    assert.equal(result.status, "accepted");

    // Progress notification is forwarded to the user as admin.task.progress.
    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: "task-1",
      value: {
        kind: proto.PROGRESS_KIND_END,
        type: proto.CHUNK_TYPE_TEXT,
        agent_id: "agent-1",
        task_id: "task-1",
        content: proto.textContent("done"),
        done: true,
      },
    } satisfies proto.ProgressParams));

    const progress = await userConn.next(proto.METHOD_ADMIN_PROGRESS);
    const progressParams = proto.decodeParams<proto.AdminProgressParams>(progress);
    assert.equal(progressParams.task_id, "task-1");
    assert.equal(progressParams.agent_id, "agent-1");
    assert.equal(progressParams.done, true);
  } finally {
    for (const c of conns) c.close();
    await srv.close();
  }
});

test("unauthorized user cannot manage another user's agent", async () => {
  const srv = await startGateway(testConfig());
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${srv.base}/ws/agent?token=${jwtFor("tester")}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-1");

    const otherConn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("other")}`);
    conns.push(otherConn);
    await otherConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    otherConn.send(proto.newRequest("req-1", proto.METHOD_TASK_CREATE, {
      agent_id: "agent-1",
      task_id: "task-1",
      type: "chat",
      content: "hello",
    } satisfies proto.TaskCreateParams));

    const resp = await otherConn.next();
    assert.equal(resp.id, "req-1");
    assert.equal(resp.error?.code, proto.ERR_UNAUTHORIZED);
  } finally {
    for (const c of conns) c.close();
    await srv.close();
  }
});

test("attachment upload and fetch via local store", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "am-gw-attach-"));
  const store = await createLocalAttachmentStore(dir);
  assert.ok(store);
  const srv = await startGateway(testConfig(), store);
  try {
    const base = srv.base.replace("ws://", "http://");
    const token = jwtFor("tester");

    // 未认证 → 401
    const anon = await fetch(`${base}/attachments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "a.png", mime: "image/png", data: "iVBORw0KGgo=" }),
    });
    assert.equal(anon.status, 401);

    // 上传 → 返回 url → GET 回源
    const up = await fetch(`${base}/attachments`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ name: "hello.png", mime: "image/png", data: "aGVsbG8=" }),
    });
    assert.equal(up.status, 200);
    const meta = (await up.json()) as { url: string; size: number };
    assert.ok(meta.url.startsWith("/files/attachments/tester/"));
    assert.equal(meta.size, 5);
    const got = await fetch(`${base}${meta.url}`);
    assert.equal(got.status, 200);
    assert.equal(got.headers.get("content-type"), "image/png");
    assert.equal(await got.text(), "hello");
  } finally {
    await srv.close();
    await fs.rm(dir, { recursive: true, force: true });
  }
});

test("attachments endpoint disabled without store", async () => {
  const srv = await startGateway(testConfig());
  try {
    const base = srv.base.replace("ws://", "http://");
    const resp = await fetch(`${base}/attachments`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${jwtFor("tester")}` },
      body: JSON.stringify({ name: "a.png", data: "aGVsbG8=" }),
    });
    assert.equal(resp.status, 503);
  } finally {
    await srv.close();
  }
});

test("attachment quota rejects upload when exceeded", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "am-gw-quota-"));
  const store = await createLocalAttachmentStore(dir);
  assert.ok(store);
  // 0.0001MB ≈ 104 字节，1KB 文件必然超限
  const srv = await startGateway(testConfig({ attachQuotaMb: 0.0001 }), store);
  try {
    const base = srv.base.replace("ws://", "http://");
    const resp = await fetch(`${base}/attachments`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${jwtFor("tester")}` },
      body: JSON.stringify({ name: "big.bin", mime: "application/octet-stream", data: Buffer.alloc(1024).toString("base64") }),
    });
    assert.equal(resp.status, 429);
  } finally {
    await srv.close();
    await fs.rm(dir, { recursive: true, force: true });
  }
});

test("metrics and healthz endpoints", async () => {
  const srv = await startGateway(testConfig());
  try {
    const base = srv.base.replace("ws://", "http://");
    const health = await fetch(`${base}/healthz`);
    assert.equal(health.status, 200); // 无 db/redis 时依赖为 disabled，整体 ok
    const h = (await health.json()) as { status: string; db: unknown };
    assert.equal(h.status, "ok");

    const conn = await Conn.dial(`${srv.base}/ws/admin?token=${jwtFor("tester")}`);
    await conn.next(proto.METHOD_ADMIN_AGENT_LIST);
    const metrics = await fetch(`${base}/metrics`);
    assert.equal(metrics.status, 200);
    const text = await metrics.text();
    assert.match(text, /ywm_users_connected 1/);
    assert.match(text, /ywm_tasks_created_total 0/);
    conn.close();
  } finally {
    await srv.close();
  }
});

test("login rate limited after 10 attempts per IP", async () => {
  const srv = await startGateway(testConfig());
  try {
    const base = srv.base.replace("ws://", "http://");
    let lastStatus = 0;
    for (let i = 0; i < 11; i++) {
      const resp = await fetch(`${base}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: "nobody", password: "x" }),
      });
      lastStatus = resp.status;
    }
    assert.equal(lastStatus, 429);
  } finally {
    await srv.close();
  }
});

test("invalid token rejected with 401", async () => {
  const srv = await startGateway(testConfig());
  try {
    await assert.rejects(
      new Promise((resolve, reject) => {
        const ws = new WebSocket(`${srv.base}/ws/admin?token=not-a-jwt`);
        ws.once("open", resolve);
        ws.once("error", reject);
      }),
      /401/,
    );
  } finally {
    await srv.close();
  }
});

test("pending request times out and user gets an error", async () => {
  const hub = new Hub(90_000, 120_000, 300_000, 30);
  const sent: proto.Message[] = [];
  const fakeWs = {
    readyState: WebSocket.OPEN,
    send: (d: string) => sent.push(JSON.parse(d.toString()) as proto.Message),
  } as unknown as WebSocket;
  const user = {
    ws: fakeWs, userID: "u1", lastHeartbeat: Date.now(), alive: true, isAdmin: false,
  } as unknown as Parameters<Hub["trackPendingRequest"]>[1];
  try {
    hub.trackPendingRequest("req-x", user);
    assert.equal(hub.pendingRequests.size, 1);
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(hub.pendingRequests.size, 0);
    assert.equal(sent.length, 1);
    assert.equal(sent[0].id, "req-x");
    assert.equal(sent[0].error?.message, "request timeout");
  } finally {
    hub.shutdown();
  }
});

test("delivered pending response cancels the timeout", async () => {
  const hub = new Hub(90_000, 120_000, 300_000, 60);
  const sent: proto.Message[] = [];
  const fakeWs = {
    readyState: WebSocket.OPEN,
    send: (d: string) => sent.push(JSON.parse(d.toString()) as proto.Message),
  } as unknown as WebSocket;
  const user = {
    ws: fakeWs, userID: "u1", lastHeartbeat: Date.now(), alive: true, isAdmin: false,
  } as unknown as Parameters<Hub["trackPendingRequest"]>[1];
  try {
    hub.trackPendingRequest("req-y", user);
    const ok = hub.deliverToLocalPending("req-y", proto.newResponse("req-y", { status: "ok" }));
    assert.equal(ok, true);
    await new Promise((resolve) => setTimeout(resolve, 150));
    // 只有投递的那一条响应，不应再出现超时错误
    assert.equal(sent.length, 1);
    assert.equal(sent[0].error, undefined);
  } finally {
    hub.shutdown();
  }
});
