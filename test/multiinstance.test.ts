// 双实例集成测试：两个网关共享 MySQL + Redis（独立前缀），验证跨实例
// agent 发现、task.create 路由、pending 响应回传与进度广播。
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db } from "../src/db.ts";
import { hashPassword, signJwt } from "../src/auth.ts";
import { setLogLevel } from "../src/util.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const REDIS_URL = process.env.AGENT_MANAGE_TEST_REDIS_URL ?? "redis://localhost:6379";
const JWT_SECRET = "mi-test-secret";
// 与开发环境共用 Redis，用随机前缀隔离（注册表与频道）
const PREFIX = `ywm-mi-${crypto.randomUUID().slice(0, 8)}`;

function cfgFor(instanceID: string): GatewayConfig {
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
    redisURL: REDIS_URL,
    redisPrefix: PREFIX,
    instanceID,
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
      const timer = setTimeout(() => reject(new Error(`timeout waiting for ${method ?? "message"}`)), timeoutMs);
      this.waiters.push({ method, resolve, timer });
    });
  }

  close(): void {
    this.ws.close();
  }
}

interface Running {
  base: string;
  close: () => Promise<void>;
}

async function startGw(instanceID: string, db: Db): Promise<Running> {
  const { server, hub } = await createGatewayServer(cfgFor(instanceID), STATIC_FILE, db);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    base: `ws://localhost:${port}`,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => {
          // 关掉 bus 的 Redis 连接，否则测试进程无法退出
          void hub.bus?.stop().catch(() => {}).then(() => resolve());
        });
      }),
  };
}

test("two instances route tasks via redis bus", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过双实例测试");
    await db.close().catch(() => {});
    return;
  }
  const uid = "mi-" + crypto.randomUUID();
  await db.createUser({ id: uid, name: uid, password_hash: hashPassword("pw") });
  const token = signJwt({ sub: uid, name: uid }, JWT_SECRET, 60_000);
  const taskID = "mi-task-" + crypto.randomUUID().slice(0, 8);
  // 品牌目录非空会开启治理模式导致注册被拒：快照后清空（开放模式），结束后恢复
  const savedBrands = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});

  let gw1: Running | undefined;
  let gw2: Running | undefined;
  const conns: Conn[] = [];
  try {
    gw1 = await startGw("mi-gw-1", db); // agent 侧
    gw2 = await startGw("mi-gw-2", db); // 用户侧

    // agent 注册到 gw-1
    const agent = await Conn.dial(`${gw1.base}/ws/agent?token=${token}`);
    conns.push(agent);
    agent.send(proto.newRequest("reg-1", proto.METHOD_REGISTER, {
      agent_id: "mi-agent",
      name: "mi-agent",
      capabilities: [{ type: "chat", name: "general" }],
    } satisfies proto.RegisterParams));
    const regResp = await agent.next();
    assert.equal(regResp.error, undefined);

    // 用户连到 gw-2，应通过 Redis 注册表看到 gw-1 上的 agent
    const admin = await Conn.dial(`${gw2.base}/ws/admin?token=${token}`);
    conns.push(admin);
    const list = proto.decodeParams<proto.AgentListParams>(await admin.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.ok(list.agents.some((a) => a.id === "mi-agent"), `agent list: ${JSON.stringify(list.agents)}`);

    // task.create 到达 gw-2，经总线路由到 gw-1 的 agent
    admin.send(proto.newRequest("req-1", proto.METHOD_TASK_CREATE, {
      agent_id: "mi-agent",
      task_id: taskID,
      type: "chat",
      content: "hello across instances",
    } satisfies proto.TaskCreateParams));

    const chat = await agent.next(proto.METHOD_AGENT_CHAT);
    assert.equal(chat.id, "req-1");
    const chatParams = proto.decodeParams<proto.AgentChatParams>(chat);
    assert.equal(chatParams.content, "hello across instances");

    // agent 接受：响应经 pending 广播回到 gw-2 上的等待请求
    agent.send(proto.newResponse("req-1", {
      status: "accepted",
      task_id: taskID,
    } satisfies proto.TaskAcceptResult));
    const accept = await admin.next();
    assert.equal(accept.id, "req-1");
    assert.equal((accept.result as proto.TaskAcceptResult).status, "accepted");

    // 进度通知经用户广播送达 gw-2（session_id 按协议从 agent.chat 回显）
    agent.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: taskID,
      value: {
        kind: proto.PROGRESS_KIND_END,
        type: proto.CHUNK_TYPE_TEXT,
        agent_id: "mi-agent",
        task_id: taskID,
        session_id: chatParams.session_id,
        content: proto.textContent("done"),
        done: true,
      },
    } satisfies proto.ProgressParams));
    const progress = proto.decodeParams<proto.AdminProgressParams>(await admin.next(proto.METHOD_ADMIN_PROGRESS));
    assert.equal(progress.task_id, taskID);
    assert.equal(progress.done, true);

    // 两侧落库：user 消息在 gw-2 持久化，assistant 缓冲在 gw-1 刷盘
    const deadline = Date.now() + 3000;
    let roles: string[] = [];
    while (Date.now() < deadline) {
      const session = (await db.listSessions(uid)).find((s) => s.agent_id === "mi-agent");
      if (session) {
        roles = (await db.listMessages(uid, session.id, 50)).map((m) => m.role).sort();
        if (roles.length === 2) break;
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    assert.deepEqual(roles, ["assistant", "user"]);
  } finally {
    for (const c of conns) c.close();
    await gw1?.close();
    await gw2?.close();
    for (const b of savedBrands) {
      await db.createBrand({
        id: b.id, name: b.name, description: b.description, logo_url: b.logo_url,
        capabilities: b.capabilities, launch_cmd: b.launch_cmd, conn_type: b.conn_type, endpoint: b.endpoint,
      }).catch(() => {});
    }
    await db.deleteUser(uid).catch(() => {});
    await db.close();
  }
});
