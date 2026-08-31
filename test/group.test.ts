import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db, type DbAgentBrand } from "../src/db.ts";
import { setLogLevel } from "../src/util.ts";
import { signJwt } from "../src/auth.ts";

setLogLevel(process.env.YWM_TEST_LOG ?? "error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "group-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-group-test";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "group-test",
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

// 起真实网关 + MySQL；品牌目录快照后清空（开放模式注册免审批），结束后恢复
async function startFixture(t: import("node:test").TestContext): Promise<Fixture | undefined> {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过群组测试");
    await db.close().catch(() => {});
    return undefined;
  }
  const savedBrands: DbAgentBrand[] = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});
  // 有 DB 时 WS 升级会校验用户存在且未禁用
  if (!(await db.getUserById(OWNER))) {
    await db.createUser({ id: OWNER, name: OWNER, password_hash: "x" });
  }
  if (!(await db.getUserById("grp-other"))) {
    await db.createUser({ id: "grp-other", name: "grp-other", password_hash: "x" });
  }
  // 清理历史运行残留的群（失败中断时不一定走到 finally）
  for (const g of await db.listGroups(OWNER)) await db.deleteGroup(OWNER, g.id).catch(() => {});
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

test("group CRUD and owner isolation", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  try {
    await upsertAgentRow(db, "grp-a1", OWNER);
    await upsertAgentRow(db, "grp-a2", OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("gc-1", proto.METHOD_GROUP_CREATE, {
      name: "研发群",
      agent_ids: ["grp-a1", "grp-a2", "grp-a1"],
      manager_agent_id: "grp-a1",
    } satisfies proto.GroupCreateParams));
    const created = await userConn.next();
    assert.equal(created.error, undefined, JSON.stringify(created.error));
    const groupID = (created.result as proto.GroupCreateResult).group_id;

    userConn.send(proto.newRequest("gl-1", proto.METHOD_GROUP_LIST, {}));
    const listed = await userConn.next();
    const groups = (listed.result as proto.GroupListResult).groups;
    assert.equal(groups.length, 1);
    assert.deepEqual(groups[0].agent_ids, ["grp-a1", "grp-a2"]); // 去重
    assert.equal(groups[0].manager_agent_id, "grp-a1");

    // detail
    userConn.send(proto.newRequest("gd-1", proto.METHOD_GROUP_DETAIL, { group_id: groupID } satisfies proto.GroupDetailParams));
    const detail = await userConn.next();
    assert.equal((detail.result as proto.GroupDetailResult).group.name, "研发群");

    // add / remove
    await upsertAgentRow(db, "grp-a3", OWNER);
    userConn.send(proto.newRequest("ga-1", proto.METHOD_GROUP_ADD, { group_id: groupID, agent_id: "grp-a3" } satisfies proto.GroupAddParams));
    assert.equal((await userConn.next()).error, undefined);
    userConn.send(proto.newRequest("gr-1", proto.METHOD_GROUP_REMOVE, { group_id: groupID, agent_id: "grp-a3" } satisfies proto.GroupRemoveParams));
    assert.equal((await userConn.next()).error, undefined);

    // owner 隔离：他人看不到
    const otherConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor("grp-other")}`);
    conns.push(otherConn);
    await otherConn.next(proto.METHOD_ADMIN_AGENT_LIST); // 连接建立即推的 agent 列表
    otherConn.send(proto.newRequest("glo-1", proto.METHOD_GROUP_LIST, {}));
    const otherList = await otherConn.next();
    assert.equal((otherList.result as proto.GroupListResult).groups.length, 0);
    otherConn.send(proto.newRequest("gdo-1", proto.METHOD_GROUP_DETAIL, { group_id: groupID } satisfies proto.GroupDetailParams));
    assert.equal((await otherConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    // 非属主 agent 不能入群
    await upsertAgentRow(db, "grp-foreign", "someone-else");
    userConn.send(proto.newRequest("ga-2", proto.METHOD_GROUP_ADD, { group_id: groupID, agent_id: "grp-foreign" } satisfies proto.GroupAddParams));
    assert.equal((await userConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    // rename / set_manager / delete
    userConn.send(proto.newRequest("grn-1", proto.METHOD_GROUP_RENAME, { group_id: groupID, name: "  新名字 " } satisfies proto.GroupRenameParams));
    assert.equal((await userConn.next()).error, undefined);
    userConn.send(proto.newRequest("gsm-1", proto.METHOD_GROUP_SET_MANAGER, { group_id: groupID, manager_agent_id: "grp-a2" } satisfies proto.GroupSetManagerParams));
    assert.equal((await userConn.next()).error, undefined);
    userConn.send(proto.newRequest("gsm-2", proto.METHOD_GROUP_SET_MANAGER, { group_id: groupID, manager_agent_id: "grp-foreign" } satisfies proto.GroupSetManagerParams));
    assert.equal((await userConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    // 他人 rename/delete → 属主隔离
    otherConn.send(proto.newRequest("grn-o", proto.METHOD_GROUP_RENAME, { group_id: groupID, name: "劫持" } satisfies proto.GroupRenameParams));
    assert.equal((await otherConn.next()).error?.code, proto.ERR_INVALID_PARAMS);
    otherConn.send(proto.newRequest("gdel-o", proto.METHOD_GROUP_DELETE, { group_id: groupID } satisfies proto.GroupDeleteParams));
    assert.equal((await otherConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    userConn.send(proto.newRequest("gl-2", proto.METHOD_GROUP_LIST, {}));
    const after = (await userConn.next()).result as proto.GroupListResult;
    assert.equal(after.groups[0].name, "新名字");
    assert.equal(after.groups[0].manager_agent_id, "grp-a2");

    userConn.send(proto.newRequest("gdel-1", proto.METHOD_GROUP_DELETE, { group_id: groupID } satisfies proto.GroupDeleteParams));
    assert.equal((await userConn.next()).error, undefined);
    userConn.send(proto.newRequest("gl-3", proto.METHOD_GROUP_LIST, {}));
    assert.equal(((await userConn.next()).result as proto.GroupListResult).groups.length, 0);

    await db.deleteGroup(OWNER, groupID).catch(() => {});
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("group task.create fan-out, attribution and cancel", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  try {
    // 两个在线 agent（注册即落库，属 OWNER）
    const a1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const a2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(a1, a2);
    await registerAgent(a1, "fan-a1");
    await registerAgent(a2, "fan-a2");
    await upsertAgentRow(db, "fan-a1", OWNER);
    await upsertAgentRow(db, "fan-a2", OWNER);

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("gc-1", proto.METHOD_GROUP_CREATE, {
      name: "fan", agent_ids: ["fan-a1", "fan-a2"],
    } satisfies proto.GroupCreateParams));
    groupID = ((await userConn.next()).result as proto.GroupCreateResult).group_id;

    // 无 mentions → 拒绝（默认不触发）
    userConn.send(proto.newRequest("t0", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-gt-0`, type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams));
    assert.equal((await userConn.next()).error?.code, proto.ERR_INVALID_PARAMS);

    // @全体 → fan-out，网关立即应答
    userConn.send(proto.newRequest("t1", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-gt-1`, type: "chat", content: "hi all", mentions: ["all"],
    } satisfies proto.TaskCreateParams));
    const accepted = await userConn.next();
    assert.equal(accepted.error, undefined, JSON.stringify(accepted.error));
    const acc = accepted.result as { task_id: string; task_ids: string[]; group_id: string };
    assert.deepEqual(acc.task_ids, [`${rid}-gt-1#0`, `${rid}-gt-1#1`]);
    assert.equal(acc.group_id, groupID);

    const chat1 = proto.decodeParams<proto.AgentChatParams>(await a1.next(proto.METHOD_AGENT_CHAT));
    const chat2 = proto.decodeParams<proto.AgentChatParams>(await a2.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat1.task_id, `${rid}-gt-1#0`);
    assert.equal(chat2.task_id, `${rid}-gt-1#1`);
    assert.equal(chat1.agent_id, "fan-a1"); // connector 注入
    // 群上下文注入：agent 可感知群成员与管理者
    const g1 = (chat1.metadata?.group ?? {}) as {
      group_id: string; manager_agent_id: string | null;
      members: Array<{ agent_id: string }>; mentions: string[];
    };
    assert.equal(g1.group_id, groupID);
    assert.deepEqual(g1.members?.map((m) => m.agent_id).sort(), ["fan-a1", "fan-a2"]);
    assert.deepEqual(g1.mentions?.sort(), ["fan-a1", "fan-a2"]);
    assert.equal(g1.manager_agent_id, null);

    // 两 agent 各自回进度（带 group_id 归因）
    for (const [conn, tid, agent] of [[a1, `${rid}-gt-1#0`, "fan-a1"], [a2, `${rid}-gt-1#1`, "fan-a2"]] as const) {
      conn.send(proto.newNotification(proto.METHOD_PROGRESS, {
        token: tid,
        value: {
          kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
          agent_id: agent, task_id: tid, content: proto.textContent(`reply from ${agent}`), done: true,
        },
      } satisfies proto.ProgressParams));
    }
    const p1 = proto.decodeParams<proto.AdminProgressParams>(await userConn.next(proto.METHOD_ADMIN_PROGRESS));
    const p2 = proto.decodeParams<proto.AdminProgressParams>(await userConn.next(proto.METHOD_ADMIN_PROGRESS));
    assert.equal(p1.group_id, groupID);
    assert.equal(p2.group_id, groupID);
    assert.deepEqual(new Set([p1.agent_id, p2.agent_id]), new Set(["fan-a1", "fan-a2"]));

    // 单 @ → 单目标，task_id 不派生
    userConn.send(proto.newRequest("t2", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-gt-2`, type: "chat", content: "hi a2", mentions: ["fan-a2"],
    } satisfies proto.TaskCreateParams));
    const acc2 = (await userConn.next()).result as { task_ids: string[] };
    assert.deepEqual(acc2.task_ids, [`${rid}-gt-2`]);
    const chat3 = proto.decodeParams<proto.AgentChatParams>(await a2.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat3.task_id, `${rid}-gt-2`);
    a2.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: `${rid}-gt-2`,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: "fan-a2", task_id: `${rid}-gt-2`, content: proto.textContent("solo reply"), done: true,
      },
    } satisfies proto.ProgressParams));
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);

    // 落库归因：1 条 user（group:<gid>）+ 3 条 assistant（各 agent）
    const sessionID = chat1.session_id ?? "";
    assert.ok(sessionID);
    await waitFor(async () => (await db.countMessages(OWNER, sessionID)) >= 4);
    const session = await db.getSession(OWNER, sessionID);
    assert.equal(session?.agent_id, `group:${groupID}`);
    const msgs = await db.listMessages(OWNER, sessionID, 50);
    assert.equal(msgs.filter((m) => m.role === "user").length, 1);
    assert.equal(msgs.filter((m) => m.role === "user")[0].agent_id, `group:${groupID}`);
    assert.deepEqual(
      new Set(msgs.filter((m) => m.role === "assistant").map((m) => m.agent_id)),
      new Set(["fan-a1", "fan-a2"]),
    );

    // 群级取消：fan-out 家族任务都收到 agent.cancel
    userConn.send(proto.newRequest("t3", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-gt-3`, type: "chat", content: "cancel me", mentions: ["all"],
    } satisfies proto.TaskCreateParams));
    await userConn.next();
    await a1.next(proto.METHOD_AGENT_CHAT);
    await a2.next(proto.METHOD_AGENT_CHAT);
    userConn.send(proto.newRequest("tc-1", proto.METHOD_TASK_CANCEL, {
      group_id: groupID, task_id: `${rid}-gt-3`,
    } satisfies proto.TaskCancelParams));
    const cancelResp = await userConn.next();
    assert.equal(cancelResp.error, undefined, JSON.stringify(cancelResp.error));
    const c1 = proto.decodeParams<proto.AgentCancelParams>(await a1.next(proto.METHOD_AGENT_CANCEL));
    const c2 = proto.decodeParams<proto.AgentCancelParams>(await a2.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(c1.task_id, `${rid}-gt-3#0`);
    assert.equal(c2.task_id, `${rid}-gt-3#1`);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

test("manager agent orchestration", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  try {
    const manager = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const worker = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(manager, worker);
    await registerAgent(manager, "mgr-1");
    await registerAgent(worker, "wrk-1");
    await upsertAgentRow(db, "mgr-1", OWNER);
    await upsertAgentRow(db, "wrk-1", OWNER);

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("gc-1", proto.METHOD_GROUP_CREATE, {
      name: "orch", agent_ids: ["mgr-1", "wrk-1"], manager_agent_id: "mgr-1",
    } satisfies proto.GroupCreateParams));
    groupID = ((await userConn.next()).result as proto.GroupCreateResult).group_id;

    // @管理者 → 管理者收到任务
    userConn.send(proto.newRequest("ot", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-ot-1`, type: "chat", content: "帮我调研", mentions: ["mgr-1"],
    } satisfies proto.TaskCreateParams));
    await userConn.next();
    const chat = proto.decodeParams<proto.AgentChatParams>(await manager.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat.task_id, `${rid}-ot-1`);
    // 管理者能从 metadata.group 感知自己是 leader 与群成员
    const mg = (chat.metadata?.group ?? {}) as { manager_agent_id: string; mentions: string[] };
    assert.equal(mg.manager_agent_id, "mgr-1");
    assert.deepEqual(mg.mentions, ["mgr-1"]);
    // 绑定会话工作目录：后续编排子任务应继承注入
    await db.setSessionWorkdir(OWNER, chat.session_id ?? "", "/tmp/orch-dir");

    // 管理者 invoke 群内 worker
    manager.send(proto.newRequest("inv-1", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: `${rid}-ot-1`, group_id: groupID, target_agent_id: "wrk-1",
      type: "chat", content: "查一下数据",
    } satisfies proto.AgentTaskInvokeParams));
    const invResp = await manager.next();
    assert.equal(invResp.error, undefined, JSON.stringify(invResp.error));
    const childTaskID = (invResp.result as proto.AgentTaskInvokeResult).task_id;
    assert.ok(childTaskID.startsWith(`${rid}-ot-1@`));

    const subChat = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    assert.equal(subChat.task_id, childTaskID);
    assert.equal(subChat.agent_id, "wrk-1");
    assert.equal(subChat.session_id, chat.session_id); // 子任务同会话（群里可见）
    // 编排子任务同样注入群上下文，mentions = 实际派发目标
    const wg = (subChat.metadata?.group ?? {}) as { group_id: string; mentions: string[] };
    assert.equal(wg.group_id, groupID);
    assert.deepEqual(wg.mentions, ["wrk-1"]);
    // 会话绑定的 workdir 对编排子任务同样生效
    assert.equal((subChat.metadata as Record<string, unknown> | undefined)?.workdir, "/tmp/orch-dir");

    // 子任务不能再编排（未授权：仅管理者/授权 delegate 可 invoke → -32006）；须在子任务完成（untrack）前发起
    worker.send(proto.newRequest("inv-2", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: childTaskID, group_id: groupID, target_agent_id: "mgr-1",
      type: "chat", content: "nested",
    } satisfies proto.AgentTaskInvokeParams));
    assert.equal((await worker.next()).error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // worker 回流完成 → 管理者收 agent.task.result，用户收带 parent_task_id 的进度
    worker.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: childTaskID,
      value: {
        kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT,
        agent_id: "wrk-1", task_id: childTaskID, content: proto.textContent("调研结果"), done: true,
      },
    } satisfies proto.ProgressParams));
    const result = proto.decodeParams<proto.AgentTaskResultParams>(await manager.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(result.parent_task_id, `${rid}-ot-1`);
    assert.equal(result.target_agent_id, "wrk-1");
    assert.equal(result.agent_id, "mgr-1"); // 接收方管理者（connector 路由用）
    assert.equal(result.status, "completed");
    assert.ok(JSON.stringify(result.chunks).includes("调研结果"));

    const userProgress = proto.decodeParams<proto.AdminProgressParams>(await userConn.next(proto.METHOD_ADMIN_PROGRESS));
    assert.equal(userProgress.parent_task_id, `${rid}-ot-1`);
    assert.equal(userProgress.agent_id, "wrk-1");
    assert.equal(userProgress.group_id, groupID);

    // 不存在的父任务 → INVALID_PARAMS
    manager.send(proto.newRequest("inv-3", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: "not-a-task", group_id: groupID, target_agent_id: "wrk-1",
      type: "chat", content: "x",
    } satisfies proto.AgentTaskInvokeParams));
    assert.equal((await manager.next()).error?.code, proto.ERR_INVALID_PARAMS);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// C6：单 agent 路径 task.cancel（不带 group_id）同样按 parent_task_id 级联取消未完成子任务
test("single-agent task.cancel cascades to orchestration subtask", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  try {
    const manager = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const worker = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(manager, worker);
    await registerAgent(manager, "casc-mgr");
    await registerAgent(worker, "casc-wrk");
    await upsertAgentRow(db, "casc-mgr", OWNER);
    await upsertAgentRow(db, "casc-wrk", OWNER);

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("gc-1", proto.METHOD_GROUP_CREATE, {
      name: "casc", agent_ids: ["casc-mgr", "casc-wrk"], manager_agent_id: "casc-mgr",
    } satisfies proto.GroupCreateParams));
    groupID = ((await userConn.next()).result as proto.GroupCreateResult).group_id;

    userConn.send(proto.newRequest("ot", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-ct-1`, type: "chat", content: "帮我调研", mentions: ["casc-mgr"],
    } satisfies proto.TaskCreateParams));
    await userConn.next();
    await manager.next(proto.METHOD_AGENT_CHAT);

    manager.send(proto.newRequest("inv-1", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: `${rid}-ct-1`, group_id: groupID, target_agent_id: "casc-wrk",
      type: "chat", content: "查一下数据",
    } satisfies proto.AgentTaskInvokeParams));
    const invResp = await manager.next();
    assert.equal(invResp.error, undefined, JSON.stringify(invResp.error));
    const childTaskID = (invResp.result as proto.AgentTaskInvokeResult).task_id;
    await worker.next(proto.METHOD_AGENT_CHAT);

    // 不带 group_id 的单 agent 取消：父任务取消应级联子任务
    userConn.send(proto.newRequest("cancel-1", proto.METHOD_TASK_CANCEL, {
      agent_id: "casc-mgr", task_id: `${rid}-ct-1`,
    } satisfies proto.TaskCancelParams));
    const cancelResp = await userConn.next();
    assert.equal(cancelResp.error, undefined, JSON.stringify(cancelResp.error));

    // 管理者收到父任务的 agent.cancel
    const parentCancel = proto.decodeParams<proto.AgentCancelParams>(
      await manager.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(parentCancel.task_id, `${rid}-ct-1`);
    // worker 收到子任务的 agent.cancel（级联）
    const childCancel = proto.decodeParams<proto.AgentCancelParams>(
      await worker.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(childCancel.task_id, childTaskID);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// 离线成员：@全体 跳过离线目标（响应带 skipped_offline），全部离线直接拒绝
test("group fan-out skips offline members", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  try {
    // 仅一个在线 agent；另一个只在库里（离线）
    const a1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(a1);
    await registerAgent(a1, "skip-a1");
    await upsertAgentRow(db, "skip-a1", OWNER);
    await upsertAgentRow(db, "skip-a2", OWNER);

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    userConn.send(proto.newRequest("gc-1", proto.METHOD_GROUP_CREATE, {
      name: "skip", agent_ids: ["skip-a1", "skip-a2"],
    } satisfies proto.GroupCreateParams));
    groupID = ((await userConn.next()).result as proto.GroupCreateResult).group_id;

    // @全体 → 只派发在线的 skip-a1，响应声明跳过 skip-a2
    userConn.send(proto.newRequest("t1", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-st-1`, type: "chat", content: "hi all", mentions: ["all"],
    } satisfies proto.TaskCreateParams));
    const acc = await userConn.next();
    assert.equal(acc.error, undefined, JSON.stringify(acc.error));
    const res = acc.result as { task_ids: string[]; skipped_offline?: string[] };
    assert.deepEqual(res.task_ids, [`${rid}-st-1`]); // 单在线目标不派生 #n
    assert.deepEqual(res.skipped_offline, ["skip-a2"]);
    const chat = proto.decodeParams<proto.AgentChatParams>(await a1.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat.task_id, `${rid}-st-1`);
    // metadata.group.mentions 只含实际派发目标
    const g = (chat.metadata?.group ?? {}) as { mentions: string[] };
    assert.deepEqual(g.mentions, ["skip-a1"]);
    a1.close();
    conns.splice(conns.indexOf(a1), 1);
    await new Promise((r) => setTimeout(r, 200)); // 等 gateway 清理连接

    // @离线成员单挑 → 全离线 -32000（按 id 过滤，避开 a1 断开触发的 agentList 广播）
    const rpcID = async (id: string, method: string, params: object): Promise<proto.Message> => {
      userConn.send(proto.newRequest(id, method, params));
      for (;;) {
        const m = await userConn.next();
        if (m.id === id) return m;
      }
    };
    const miss = await rpcID("t2", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-st-2`, type: "chat", content: "hi", mentions: ["skip-a2"],
    } satisfies proto.TaskCreateParams);
    assert.equal(miss.error?.code, proto.ERR_AGENT_NOT_FOUND);

    // 两个都离线 @全体 → 同样 -32000
    const allMiss = await rpcID("t3", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: `${rid}-st-3`, type: "chat", content: "hi", mentions: ["all"],
    } satisfies proto.TaskCreateParams);
    assert.equal(allMiss.error?.code, proto.ERR_AGENT_NOT_FOUND);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

async function waitFor(fn: () => Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}

// 断言一段时间内没有任何消息到达（验证幂等去重没有产生重复派发）
async function expectSilence(conn: Conn, ms = 300): Promise<void> {
  await assert.rejects(() => conn.next(undefined, ms), /timeout/);
}

// 按 id 等待 RPC 响应（跳过路上插进来的通知帧，如防抖后的 admin.agentList）
async function rpc<T>(conn: Conn, id: string, method: string, params: object): Promise<T> {
  conn.send(proto.newRequest(id, method, params));
  for (;;) {
    const m = await conn.next(undefined, 5000);
    if (m.id === id) return (m.error ? m : m.result) as T;
  }
}

// 编排夹具：管理者 + 单 worker 群，@管理者发一轮任务
async function orchestrationFixture(
  base: string, db: InstanceType<typeof Db>, prefix: string,
): Promise<{ groupID: string; parentTaskID: string; manager: Conn; worker: Conn; userConn: Conn; conns: Conn[] }> {
  const manager = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
  const worker = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
  const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
  const conns = [manager, worker, userConn];
  await registerAgent(manager, `${prefix}-mgr`);
  await registerAgent(worker, `${prefix}-wrk`);
  await upsertAgentRow(db, `${prefix}-mgr`, OWNER);
  await upsertAgentRow(db, `${prefix}-wrk`, OWNER);
  await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
  const groupID = (await rpc<proto.GroupCreateResult>(userConn, `${prefix}-gc`, proto.METHOD_GROUP_CREATE, {
    name: prefix, agent_ids: [`${prefix}-mgr`, `${prefix}-wrk`], manager_agent_id: `${prefix}-mgr`,
  } satisfies proto.GroupCreateParams)).group_id;
  const parentTaskID = `${prefix}-pt`;
  await rpc(userConn, `${prefix}-t`, proto.METHOD_TASK_CREATE, {
    group_id: groupID, task_id: parentTaskID, type: "chat", content: "帮我调研", mentions: [`${prefix}-mgr`],
  } satisfies proto.TaskCreateParams);
  await manager.next(proto.METHOD_AGENT_CHAT);
  return { groupID, parentTaskID, manager, worker, userConn, conns };
}

// worker 用一段 chunks 流终结子任务（thinking 过程 + 终态文本）
function completeTask(conn: Conn, agentID: string, taskID: string, finalText: string): void {
  conn.send(proto.newNotification(proto.METHOD_PROGRESS, {
    token: taskID,
    value: { kind: proto.PROGRESS_KIND_REPORT, type: proto.CHUNK_TYPE_THINKING, agent_id: agentID, task_id: taskID, content: proto.textContent("思考过程…") },
  } satisfies proto.ProgressParams));
  conn.send(proto.newNotification(proto.METHOD_PROGRESS, {
    token: taskID,
    value: { kind: proto.PROGRESS_KIND_END, type: proto.CHUNK_TYPE_TEXT, agent_id: agentID, task_id: taskID, content: proto.textContent(finalText), done: true },
  } satisfies proto.ProgressParams));
}

// P0-1：invocation_id 幂等（运行中去重 + 结束后重发）、context_policy 默认 final_only
test("invoke invocation_id idempotency and final_only context policy", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const fx2 = await orchestrationFixture(base, db, `idem-${rid}`);
  const { groupID, parentTaskID, manager, worker, userConn, conns } = fx2;
  try {
    // 首次 invoke（默认 final_only）
    manager.send(proto.newRequest("inv-a", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: `idem-${rid}-wrk`,
      invocation_id: "step-1", type: "chat", content: "查数据",
    } satisfies proto.AgentTaskInvokeParams));
    const r1 = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.equal(r1.status, "dispatched");
    assert.equal(r1.context_policy, "final_only");
    const childTaskID = r1.task_id;

    // 重复 invoke（子任务运行中）→ 同 task_id，不再派发
    manager.send(proto.newRequest("inv-a2", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: `idem-${rid}-wrk`,
      invocation_id: "step-1", type: "chat", content: "查数据",
    } satisfies proto.AgentTaskInvokeParams));
    const r2 = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.equal(r2.task_id, childTaskID);
    assert.equal(r2.status, "dispatched");

    const subChat = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    assert.equal(subChat.task_id, childTaskID);
    await expectSilence(worker); // 没有第二个 agent.chat

    // 子任务完成（含 thinking 过程 chunk）→ 回投只带终态文本
    completeTask(worker, `idem-${rid}-wrk`, childTaskID, "最终结论：数据没问题");
    const result = proto.decodeParams<proto.AgentTaskResultParams>(await manager.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(result.status, "completed");
    assert.equal(result.context_policy, "final_only");
    assert.equal(result.invocation_id, "step-1");
    assert.ok(JSON.stringify(result.chunks).includes("最终结论"));
    assert.ok(!JSON.stringify(result.chunks).includes("思考过程"));

    // 子任务已结束后重复 invoke → 重发终态结果 + status=duplicate，不重新派发
    manager.send(proto.newRequest("inv-a3", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: `idem-${rid}-wrk`,
      invocation_id: "step-1", type: "chat", content: "查数据",
    } satisfies proto.AgentTaskInvokeParams));
    const replay = proto.decodeParams<proto.AgentTaskResultParams>(await manager.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.ok(JSON.stringify(replay.chunks).includes("最终结论"));
    const r3 = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.equal(r3.status, "duplicate");
    assert.equal(r3.task_id, childTaskID);
    await expectSilence(worker);

    // context_policy=full → 回投全量 chunks（含 thinking）
    manager.send(proto.newRequest("inv-b", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: `idem-${rid}-wrk`,
      invocation_id: "step-2", context_policy: "full", type: "chat", content: "再来一次",
    } satisfies proto.AgentTaskInvokeParams));
    const rb = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.equal(rb.context_policy, "full");
    const chatB = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    completeTask(worker, `idem-${rid}-wrk`, chatB.task_id, "第二次结论");
    const resultB = proto.decodeParams<proto.AgentTaskResultParams>(await manager.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(resultB.context_policy, "full");
    assert.ok(JSON.stringify(resultB.chunks).includes("思考过程"));
    assert.ok(JSON.stringify(resultB.chunks).includes("第二次结论"));
    void userConn;
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// P0-1：thread_id 续聊（同线程复用子会话 id，落库归因仍在群会话）+ timeout_ms 覆盖
test("invoke thread continuation and per-invoke timeout", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const wrk = `thr-${rid}-wrk`;
  const { groupID, parentTaskID, manager, worker, conns } = await orchestrationFixture(base, db, `thr-${rid}`);
  try {
    // 第一次带 thread_id 的调用
    manager.send(proto.newRequest("inv-t1", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "t-1", thread_id: "research", type: "chat", content: "先查A",
    } satisfies proto.AgentTaskInvokeParams));
    const r1 = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.ok(r1.thread_session_id, "响应应带 thread_session_id");
    const chat1 = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat1.session_id, r1.thread_session_id);
    completeTask(worker, wrk, chat1.task_id, "A的结果");
    await manager.next(proto.METHOD_AGENT_TASK_RESULT);

    // 同 thread_id 第二次调用 → 目标 agent 见同一子会话（上下文连续）
    manager.send(proto.newRequest("inv-t2", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "t-2", thread_id: "research", type: "chat", content: "再查B",
    } satisfies proto.AgentTaskInvokeParams));
    const r2 = (await manager.next()).result as proto.AgentTaskInvokeResult;
    assert.equal(r2.thread_session_id, r1.thread_session_id);
    const chat2 = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat2.session_id, chat1.session_id);
    completeTask(worker, wrk, chat2.task_id, "B的结果");
    await manager.next(proto.METHOD_AGENT_TASK_RESULT);

    // 落库归因：两次子任务输出都落在群会话（thread 子会话不落库、不进侧栏）
    const sessionID = await groupSessionIdOf(db, groupID);
    assert.ok(sessionID);
    await waitFor(async () => (await db.countMessages(OWNER, sessionID)) >= 3); // 1 user + 2 assistant
    const msgs = await db.listMessages(OWNER, sessionID, 50);
    assert.equal(msgs.filter((m) => m.role === "assistant" && m.agent_id === wrk).length, 2);

    // timeout_ms 覆盖：worker 不应答 → 1s 超时，manager 收 failed 结果，worker 收 agent.cancel
    manager.send(proto.newRequest("inv-t3", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "t-3", timeout_ms: 1000, type: "chat", content: "永不回复",
    } satisfies proto.AgentTaskInvokeParams));
    await manager.next();
    const chat3 = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    const timeoutResult = proto.decodeParams<proto.AgentTaskResultParams>(await manager.next(proto.METHOD_AGENT_TASK_RESULT, 10_000));
    assert.equal(timeoutResult.status, "failed");
    assert.equal(timeoutResult.task_id, chat3.task_id);
    assert.ok((timeoutResult.error ?? "").includes("超时"));
    const cancel = proto.decodeParams<proto.AgentCancelParams>(await worker.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(cancel.task_id, chat3.task_id);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// 找群会话 id（sessions.agent_id = group:<gid>）
async function groupSessionIdOf(db: InstanceType<typeof Db>, groupID: string): Promise<string | undefined> {
  const sessions = await db.listSessions(OWNER, `group:${groupID}`);
  return sessions[0]?.id;
}

// P0-2：群黑板 recent_turns 注入（fan-out 与编排子任务统一）
test("group blackboard injects recent_turns", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  const a1 = `bb-${rid}-a1`;
  const a2 = `bb-${rid}-a2`;
  try {
    const c1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(c1, c2);
    await registerAgent(c1, a1);
    await registerAgent(c2, a2);
    await upsertAgentRow(db, a1, OWNER);
    await upsertAgentRow(db, a2, OWNER);

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "bb", agent_ids: [a1, a2], manager_agent_id: a1,
    } satisfies proto.GroupCreateParams)).group_id;
    // 稳定 session_id（同 UI：一个群会话一个 id），轮次才会累积在同一会话里
    const sessionID = `bb-sess-${rid}`;

    // 第一轮：@全体，两个成员回复
    const turn1 = `${rid}-bb-1`;
    await rpc(userConn, "t1", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: turn1, session_id: sessionID, type: "chat", content: "第一轮问题", mentions: ["all"],
    } satisfies proto.TaskCreateParams);
    const chat1a = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    const chat1b = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    // 首轮无历史：不注入 recent_turns
    assert.equal((chat1a.metadata?.group as { recent_turns?: unknown[] }).recent_turns, undefined);
    // 成员档案（Agent Card 内化）：capabilities + 在线态
    const members1 = (chat1a.metadata?.group as {
      members: Array<{ agent_id: string; capabilities?: Array<{ type: string }>; online: boolean }>;
    }).members;
    const mem1 = new Map(members1.map((m) => [m.agent_id, m]));
    assert.deepEqual(mem1.get(a1)?.capabilities?.map((c) => c.type), ["chat"]);
    assert.equal(mem1.get(a1)?.online, true);
    assert.equal(mem1.get(a2)?.online, true);
    completeTask(c1, a1, chat1a.task_id, "第一轮A回答");
    completeTask(c2, a2, chat1b.task_id, "第一轮B回答");
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);
    await userConn.next(proto.METHOD_ADMIN_PROGRESS);
    await waitFor(async () => (await db.countMessages(OWNER, sessionID)) >= 3);

    // 第二轮：@a1 → metadata.group.recent_turns 带第一轮完整摘要
    const turn2 = `${rid}-bb-2`;
    await rpc(userConn, "t2", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: turn2, session_id: sessionID, type: "chat", content: "第二轮问题", mentions: [a1],
    } satisfies proto.TaskCreateParams);
    const chat2a = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    const g2 = (chat2a.metadata?.group ?? {}) as {
      turn_id?: string;
      recent_turns?: Array<{ turn_id: string; user_text: string; replies: Array<{ agent_id: string; text: string }> }>;
    };
    assert.equal(g2.turn_id, turn2);
    assert.equal(g2.recent_turns?.length, 1);
    const prev = g2.recent_turns![0];
    assert.equal(prev.turn_id, turn1);
    assert.ok(prev.user_text.includes("第一轮问题"));
    const replyOf = new Map(prev.replies.map((r) => [r.agent_id, r.text]));
    assert.ok((replyOf.get(a1) ?? "").includes("第一轮A回答"));
    assert.ok((replyOf.get(a2) ?? "").includes("第一轮B回答"));

    // 编排子任务同样注入：a1（管理者，正处理 turn2）invoke a2
    c1.send(proto.newRequest("mi", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: turn2, group_id: groupID, target_agent_id: a2,
      invocation_id: "step-1", type: "chat", content: "委派",
    } satisfies proto.AgentTaskInvokeParams));
    await c1.next();
    const subChat = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    const gSub = (subChat.metadata?.group ?? {}) as { recent_turns?: unknown[] };
    assert.equal(gSub.recent_turns?.length, 1);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// P0-3：每父任务并发上限 + group.delete / group.remove 级联取消
test("per-parent subtask cap and group lifecycle cascade cancel", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const conns: Conn[] = [];
  let groupID = "";
  const rid = crypto.randomUUID().slice(0, 8);
  const wrk = `cap-${rid}-wrk`;
  try {
    const mgr = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const worker = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(mgr, worker);
    await registerAgent(mgr, `cap-${rid}-mgr`);
    await registerAgent(worker, wrk);
    await upsertAgentRow(db, `cap-${rid}-mgr`, OWNER);
    await upsertAgentRow(db, wrk, OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    userConn.send(proto.newRequest("gc", proto.METHOD_GROUP_CREATE, {
      name: "cap", agent_ids: [`cap-${rid}-mgr`, wrk], manager_agent_id: `cap-${rid}-mgr`,
    } satisfies proto.GroupCreateParams));
    groupID = ((await userConn.next()).result as proto.GroupCreateResult).group_id;
    const parentTaskID = `${rid}-cap-pt`;
    userConn.send(proto.newRequest("t", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: parentTaskID, type: "chat", content: "go", mentions: [`cap-${rid}-mgr`],
    } satisfies proto.TaskCreateParams));
    await userConn.next();
    await mgr.next(proto.METHOD_AGENT_CHAT);

    // 连发 4 个子任务（worker 不应答，全部未决）→ 第 5 个 -32006
    for (let i = 1; i <= 4; i++) {
      mgr.send(proto.newRequest(`cap-${i}`, proto.METHOD_AGENT_TASK_INVOKE, {
        parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
        invocation_id: `s-${i}`, type: "chat", content: `job ${i}`,
      } satisfies proto.AgentTaskInvokeParams));
      const r = (await mgr.next()).result as proto.AgentTaskInvokeResult;
      assert.equal(r.status, "dispatched", `invoke #${i}`);
      await worker.next(proto.METHOD_AGENT_CHAT);
    }
    mgr.send(proto.newRequest("cap-5", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "s-5", type: "chat", content: "job 5",
    } satisfies proto.AgentTaskInvokeParams));
    assert.equal((await mgr.next()).error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // group.remove：worker 出群 → 其未完成任务被取消
    userConn.send(proto.newRequest("grm", proto.METHOD_GROUP_REMOVE, { group_id: groupID, agent_id: wrk } satisfies proto.GroupRemoveParams));
    await userConn.next();
    let cancels = 0;
    for (;;) {
      try {
        const c = proto.decodeParams<proto.AgentCancelParams>(await worker.next(proto.METHOD_AGENT_CANCEL, 500));
        if (c.task_id.startsWith(`${parentTaskID}@`)) cancels++;
      } catch {
        break;
      }
    }
    assert.equal(cancels, 4, "worker 的 4 个未决子任务都应被取消");

    // group.delete：重建群再派任务，解散后 fan-out 任务被取消
    userConn.send(proto.newRequest("gc2", proto.METHOD_GROUP_CREATE, {
      name: "cap2", agent_ids: [`cap-${rid}-mgr`, wrk],
    } satisfies proto.GroupCreateParams));
    const gid2 = ((await userConn.next()).result as proto.GroupCreateResult).group_id;
    userConn.send(proto.newRequest("t2", proto.METHOD_TASK_CREATE, {
      group_id: gid2, task_id: `${rid}-cap2`, type: "chat", content: "hi", mentions: ["all"],
    } satisfies proto.TaskCreateParams));
    await userConn.next();
    await mgr.next(proto.METHOD_AGENT_CHAT);
    await worker.next(proto.METHOD_AGENT_CHAT);
    userConn.send(proto.newRequest("gdel", proto.METHOD_GROUP_DELETE, { group_id: gid2 } satisfies proto.GroupDeleteParams));
    await userConn.next();
    const cancelMgr = proto.decodeParams<proto.AgentCancelParams>(await mgr.next(proto.METHOD_AGENT_CANCEL));
    const cancelWrk = proto.decodeParams<proto.AgentCancelParams>(await worker.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(cancelMgr.task_id, `${rid}-cap2#0`);
    assert.equal(cancelWrk.task_id, `${rid}-cap2#1`);
    await db.deleteGroup(OWNER, gid2).catch(() => {});
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// P1-4：编排子任务落库（durable run tree）——dispatch 写入、终态更新、取消收口、重启恢复、run.list 查询
test("orchestration runs are durable and queryable", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const wrk = `run-${rid}-wrk`;
  const { groupID, parentTaskID, manager, worker, userConn, conns } = await orchestrationFixture(base, db, `run-${rid}`);
  try {
    // 子任务 1：完成 → run 行 completed
    const r1 = (await rpc(manager, "inv-r1", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "r-1", type: "chat", content: "job1",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    const chat1 = proto.decodeParams<proto.AgentChatParams>(await worker.next(proto.METHOD_AGENT_CHAT));
    completeTask(worker, wrk, chat1.task_id, "结果一");
    await manager.next(proto.METHOD_AGENT_TASK_RESULT);

    // 子任务 2：保持 running → group.remove 级联取消 → run 行 cancelled
    const r2 = (await rpc(manager, "inv-r2", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: parentTaskID, group_id: groupID, target_agent_id: wrk,
      invocation_id: "r-2", type: "chat", content: "job2",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    await worker.next(proto.METHOD_AGENT_CHAT);
    await rpc(userConn, "grm", proto.METHOD_GROUP_REMOVE, { group_id: groupID, agent_id: wrk } satisfies proto.GroupRemoveParams);
    await worker.next(proto.METHOD_AGENT_CANCEL);

    // run.list（owner 范围）：按父任务查派发树
    await waitFor(async () => {
      const res = (await rpc(userConn, "rl", proto.METHOD_RUN_LIST, {
        parent_task_id: parentTaskID,
      } satisfies proto.RunListParams)) as proto.RunListResult;
      return res.runs.length === 2 && res.runs.every((r) => r.status !== "running");
    });
    const runs = ((await rpc(userConn, "rl2", proto.METHOD_RUN_LIST, {
      parent_task_id: parentTaskID,
    } satisfies proto.RunListParams)) as proto.RunListResult).runs;
    const byTask = new Map(runs.map((r) => [r.task_id, r]));
    assert.equal(byTask.get(r1.task_id)?.status, "completed");
    assert.equal(byTask.get(r1.task_id)?.target_agent_id, wrk);
    assert.equal(byTask.get(r1.task_id)?.invoker_agent_id, `run-${rid}-mgr`);
    assert.equal(byTask.get(r1.task_id)?.invocation_id, "r-1");
    assert.equal(byTask.get(r2.task_id)?.status, "cancelled");

    // 迟到的 done 不覆盖 cancelled 终态（finishRun 只从 running 迁出）
    completeTask(worker, wrk, r2.task_id, "迟到的结果");
    await new Promise((r) => setTimeout(r, 200));
    const after = ((await rpc(userConn, "rl3", proto.METHOD_RUN_LIST, {
      parent_task_id: parentTaskID,
    } satisfies proto.RunListParams)) as proto.RunListResult).runs;
    assert.equal(after.find((x) => x.task_id === r2.task_id)?.status, "cancelled");

    // 启动恢复：本实例残留 running + 超龄孤儿 running → failed
    await db.createRun({
      id: `${rid}-ghost-own`, owner_id: OWNER, group_id: groupID, parent_task_id: `${rid}-ghost-p`,
      invoker_agent_id: "m", target_agent_id: "w", invocation_id: null, session_id: "",
      instance_id: "group-test", status: "running", created_at: Date.now(),
    });
    await db.createRun({
      id: `${rid}-ghost-old`, owner_id: OWNER, group_id: groupID, parent_task_id: `${rid}-ghost-p`,
      invoker_agent_id: "m", target_agent_id: "w", invocation_id: null, session_id: "",
      instance_id: "gone-instance", status: "running", created_at: Date.now() - 7_200_000,
    });
    const recovered = await db.recoverRuns("group-test", Date.now() - 3_600_000, "gateway restarted");
    assert.equal(recovered, 2);
    const ghostRuns = ((await rpc(userConn, "rl4", proto.METHOD_RUN_LIST, {
      parent_task_id: `${rid}-ghost-p`,
    } satisfies proto.RunListParams)) as proto.RunListResult).runs;
    assert.equal(ghostRuns.length, 2);
    assert.ok(ghostRuns.every((r) => r.status === "failed" && r.error === "gateway restarted"));
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// P1-5：批量 invoke targets[] + collect 策略（first / quorum 收割其余）
test("batch invoke with collect strategy", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const mgr = `bat-${rid}-mgr`;
  const w1 = `bat-${rid}-w1`;
  const w2 = `bat-${rid}-w2`;
  const w3 = `bat-${rid}-w3`;
  const conns: Conn[] = [];
  let groupID = "";
  try {
    const mc = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c3 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(mc, c1, c2, c3);
    for (const [conn, id] of [[mc, mgr], [c1, w1], [c2, w2], [c3, w3]] as const) await registerAgent(conn, id);
    for (const id of [mgr, w1, w2, w3]) await upsertAgentRow(db, id, OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "batch", agent_ids: [mgr, w1, w2, w3], manager_agent_id: mgr,
    } satisfies proto.GroupCreateParams)).group_id;
    const pt = `${rid}-bat-pt`;
    await rpc(userConn, "t", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: pt, type: "chat", content: "go", mentions: [mgr],
    } satisfies proto.TaskCreateParams);
    await mc.next(proto.METHOD_AGENT_CHAT);

    // 批量 first：w1 先完成 → w2 被收割（agent.cancel + 合成 failed 结果）
    const r1 = (await rpc(mc, "inv-1", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: pt, group_id: groupID, target_agent_id: "",
      targets: [w1, w2], invocation_id: "b-1", collect: "first",
      type: "chat", content: "race",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    assert.equal(r1.status, "dispatched");
    assert.equal(r1.tasks?.length, 2);
    const taskOf = new Map((r1.tasks ?? []).map((x) => [x.target_agent_id, x.task_id]));
    const chat1 = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    const chat2 = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat1.task_id, taskOf.get(w1));
    assert.equal(chat2.task_id, taskOf.get(w2));
    // metadata.group.mentions = 批量目标全集
    const g = (chat1.metadata?.group ?? {}) as { mentions: string[] };
    assert.deepEqual([...g.mentions].sort(), [w1, w2]);

    completeTask(c1, w1, chat1.task_id, "最快的结果");
    const res1 = proto.decodeParams<proto.AgentTaskResultParams>(await mc.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(res1.status, "completed");
    // w2 被收割：收到 agent.cancel，管理者收到合成 failed 结果
    const cancel2 = proto.decodeParams<proto.AgentCancelParams>(await c2.next(proto.METHOD_AGENT_CANCEL));
    assert.equal(cancel2.task_id, chat2.task_id);
    const res2 = proto.decodeParams<proto.AgentTaskResultParams>(await mc.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(res2.task_id, chat2.task_id);
    assert.equal(res2.status, "failed");
    assert.ok((res2.error ?? "").includes("collect"));

    // quorum(2)：三个目标，两个成功后第三个被收割
    const r2 = (await rpc(mc, "inv-2", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: pt, group_id: groupID, target_agent_id: "",
      targets: [w1, w2, w3], invocation_id: "b-2", collect: { quorum: 2 },
      type: "chat", content: "majority",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    assert.equal(r2.tasks?.length, 3);
    const q1 = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    const q2 = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    const q3 = proto.decodeParams<proto.AgentChatParams>(await c3.next(proto.METHOD_AGENT_CHAT));
    completeTask(c1, w1, q1.task_id, "票一");
    await mc.next(proto.METHOD_AGENT_TASK_RESULT); // 1/2，不收割
    await expectSilence(c3, 300);
    completeTask(c2, w2, q2.task_id, "票二");
    await mc.next(proto.METHOD_AGENT_TASK_RESULT); // w2 完成
    const harvested = proto.decodeParams<proto.AgentTaskResultParams>(await mc.next(proto.METHOD_AGENT_TASK_RESULT));
    assert.equal(harvested.task_id, q3.task_id);
    assert.equal(harvested.status, "failed");
    assert.equal((await proto.decodeParams<proto.AgentCancelParams>(await c3.next(proto.METHOD_AGENT_CANCEL))).task_id, q3.task_id);

    // 非法 quorum → -32602（rpc 返回整个错误消息对象）
    const bad = await rpc<proto.Message>(mc, "inv-3", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: pt, group_id: groupID, target_agent_id: "",
      targets: [w1], invocation_id: "b-3", collect: { quorum: 2 },
      type: "chat", content: "x",
    } satisfies proto.AgentTaskInvokeParams);
    assert.equal(bad.error?.code, proto.ERR_INVALID_PARAMS);

    // collect=all（默认）：不收割——w3 未完成时 w1 完成不触发取消
    const r3 = (await rpc(mc, "inv-4", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: pt, group_id: groupID, target_agent_id: "",
      targets: [w1, w3], invocation_id: "b-4",
      type: "chat", content: "wait all",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    assert.equal(r3.tasks?.length, 2);
    const a1 = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    await c3.next(proto.METHOD_AGENT_CHAT);
    completeTask(c1, w1, a1.task_id, "先完成");
    await mc.next(proto.METHOD_AGENT_TASK_RESULT);
    await expectSilence(c3, 300); // w3 未被取消
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// P2-8/9：delegates 授权矩阵 + 预算制多级编排（深度 ≤3、每父任务总量 ≤16）
test("delegates authorization and multi-level orchestration budget", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const mgr = `dl-${rid}-mgr`;
  const w1 = `dl-${rid}-w1`;
  const w2 = `dl-${rid}-w2`;
  const conns: Conn[] = [];
  let groupID = "";
  try {
    const mc = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(mc, c1, c2);
    for (const [conn, id] of [[mc, mgr], [c1, w1], [c2, w2]] as const) await registerAgent(conn, id);
    for (const id of [mgr, w1, w2]) await upsertAgentRow(db, id, OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "delegates", agent_ids: [mgr, w1, w2], manager_agent_id: mgr,
    } satisfies proto.GroupCreateParams)).group_id;
    const t0 = `${rid}-dl-t0`;
    await rpc(userConn, "t0", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: t0, type: "chat", content: "go", mentions: [mgr],
    } satisfies proto.TaskCreateParams);
    await mc.next(proto.METHOD_AGENT_CHAT);

    // 未授权成员不能编排：mgr 先派 w2（T1），w2 从 T1 invoke → -32006
    const r1 = (await rpc(mc, "inv-a", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t0, group_id: groupID, target_agent_id: w2, type: "chat", content: "L1",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    const t1 = r1.task_id;
    await c2.next(proto.METHOD_AGENT_CHAT);
    const denied = await rpc<proto.Message>(c2, "inv-x", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t1, group_id: groupID, target_agent_id: w1, type: "chat", content: "no",
    } satisfies proto.AgentTaskInvokeParams);
    assert.equal(denied.error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // group.set_delegates 授权 w2 → w2 从 T1 invoke w1（T2，depth 2）
    const sd = (await rpc(userConn, "gsd", proto.METHOD_GROUP_SET_DELEGATES, {
      group_id: groupID, agent_ids: [w2],
    } satisfies proto.GroupSetDelegatesParams)) as { delegate_agent_ids: string[] };
    assert.deepEqual(sd.delegate_agent_ids, [w2]);
    const r2 = (await rpc(c2, "inv-b", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t1, group_id: groupID, target_agent_id: w1, type: "chat", content: "L2",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    const t2 = r2.task_id;
    await c1.next(proto.METHOD_AGENT_CHAT);

    // w1 仍未授权 → 从 T2 invoke → -32006
    const denied2 = await rpc<proto.Message>(c1, "inv-y", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t2, group_id: groupID, target_agent_id: mgr, type: "chat", content: "no",
    } satisfies proto.AgentTaskInvokeParams);
    assert.equal(denied2.error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // 授权 w1 → 从 T2 invoke mgr（T3，depth 3 = 上限）；metadata 注入 delegates
    await rpc(userConn, "gsd2", proto.METHOD_GROUP_SET_DELEGATES, {
      group_id: groupID, agent_ids: [w1, w2],
    } satisfies proto.GroupSetDelegatesParams);
    const r3 = (await rpc(c1, "inv-c", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t2, group_id: groupID, target_agent_id: mgr, type: "chat", content: "L3",
    } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
    const t3 = r3.task_id;
    const chat3 = proto.decodeParams<proto.AgentChatParams>(await mc.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat3.task_id, t3);
    const g3 = (chat3.metadata?.group ?? {}) as { delegate_agent_ids?: string[] };
    assert.deepEqual(g3.delegate_agent_ids, [w1, w2]);

    // depth 3 的任务再编排 → -32006（深度上限）
    const tooDeep = await rpc<proto.Message>(mc, "inv-d", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t3, group_id: groupID, target_agent_id: w1, type: "chat", content: "L4",
    } satisfies proto.AgentTaskInvokeParams);
    assert.equal(tooDeep.error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // 预算制：按父任务记账——T0 名下只有 T1 与循环内派发计数（T2/T3 计入 T1/T2 的账）。
    // T1 已占 1，再派 15 个到 16，第 17 个拒绝
    for (let i = 1; i <= 15; i++) {
      const r = (await rpc(mc, `bp-${i}`, proto.METHOD_AGENT_TASK_INVOKE, {
        parent_task_id: t0, group_id: groupID, target_agent_id: w1,
        invocation_id: `bp-${i}`, type: "chat", content: `fill ${i}`,
      } satisfies proto.AgentTaskInvokeParams)) as proto.AgentTaskInvokeResult;
      const chat = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
      completeTask(c1, w1, chat.task_id, "ok");
      await mc.next(proto.METHOD_AGENT_TASK_RESULT);
      void r;
    }
    const overBudget = await rpc<proto.Message>(mc, "bp-16", proto.METHOD_AGENT_TASK_INVOKE, {
      parent_task_id: t0, group_id: groupID, target_agent_id: w1,
      invocation_id: "bp-16", type: "chat", content: "one too many",
    } satisfies proto.AgentTaskInvokeParams);
    assert.equal(overBudget.error?.code, proto.ERR_ORCHESTRATION_VIOLATION);

    // 非成员 delegate → -32602
    const badDel = await rpc<proto.Message>(userConn, "gsd3", proto.METHOD_GROUP_SET_DELEGATES, {
      group_id: groupID, agent_ids: ["not-a-member"],
    } satisfies proto.GroupSetDelegatesParams);
    assert.equal(badDel.error?.code, proto.ERR_INVALID_PARAMS);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// 声明式运行模板：round_robin 把上一步输出注入下一步输入
test("group.run round_robin threads outputs between members", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const a1 = `rr-${rid}-a1`;
  const a2 = `rr-${rid}-a2`;
  const conns: Conn[] = [];
  let groupID = "";
  try {
    const c1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(c1, c2);
    await registerAgent(c1, a1);
    await registerAgent(c2, a2);
    await upsertAgentRow(db, a1, OWNER);
    await upsertAgentRow(db, a2, OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "rr", agent_ids: [a1, a2],
    } satisfies proto.GroupCreateParams)).group_id;

    const run = (await rpc<proto.GroupRunResult>(userConn, "gr", proto.METHOD_GROUP_RUN, {
      group_id: groupID, preset: "round_robin", topic: "预算方案", rounds: 1,
    } satisfies proto.GroupRunParams));
    assert.equal(run.status, "running");
    assert.ok(run.run_id.startsWith("run-"));

    const chat1 = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat1.task_id, `${run.run_id}:0`);
    assert.ok(chat1.content?.includes("预算方案"));
    assert.ok(chat1.content?.includes("共 1 轮"));
    completeTask(c1, a1, chat1.task_id, "甲的方案A");

    // 第二位能看到第一位的发言（黑板由引擎注入，不靠 recent_turns）
    const chat2 = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    assert.equal(chat2.task_id, `${run.run_id}:1`);
    assert.ok(chat2.content?.includes("甲的方案A"), chat2.content);
    completeTask(c2, a2, chat2.task_id, "乙的补充B");

    // 落库：1 条 user（group 归因）+ 2 条 assistant
    const sessionID = chat1.session_id ?? "";
    await waitFor(async () => (await db.countMessages(OWNER, sessionID)) >= 3);
    const msgs = await db.listMessages(OWNER, sessionID, 50);
    assert.equal(msgs.filter((m) => m.role === "user")[0]?.agent_id, `group:${groupID}`);
    assert.equal(msgs.filter((m) => m.role === "assistant").length, 2);
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});

// 声明式运行模板：debate（正反+裁决）与 pipeline 自定义步骤（{{prev}} 注入、失败中止）
test("group.run debate and pipeline custom steps", async (t) => {
  const fx = await startFixture(t);
  if (!fx) return;
  const { db, base } = fx;
  const rid = crypto.randomUUID().slice(0, 8);
  const w1 = `db-${rid}-w1`;
  const w2 = `db-${rid}-w2`;
  const conns: Conn[] = [];
  let groupID = "";
  try {
    const c1 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    const c2 = await Conn.dial(`${base}/ws/agent?token=${jwtFor(OWNER)}`);
    conns.push(c1, c2);
    await registerAgent(c1, w1);
    await registerAgent(c2, w2);
    await upsertAgentRow(db, w1, OWNER);
    await upsertAgentRow(db, w2, OWNER);
    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    conns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "tpl", agent_ids: [w1, w2], manager_agent_id: w1,
    } satisfies proto.GroupCreateParams)).group_id;

    // debate：成员序 [w1, w2] → 正方 w1、反方 w2、裁决 = 管理者 w1
    const run = (await rpc<proto.GroupRunResult>(userConn, "gr", proto.METHOD_GROUP_RUN, {
      group_id: groupID, preset: "debate", topic: "是否采用微服务", rounds: 1,
    } satisfies proto.GroupRunParams));
    const pro = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    assert.ok(pro.content?.includes("正方") && pro.content?.includes("是否采用微服务"));
    completeTask(c1, w1, pro.task_id, "正方论点P");
    const con = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    assert.ok(con.content?.includes("正方论点P"), "反方应看到正方论述");
    completeTask(c2, w2, con.task_id, "反方反驳C");
    const verdict = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    assert.ok(verdict.content?.includes("正方论点P") && verdict.content?.includes("反方反驳C"));
    assert.ok(verdict.content?.includes("裁判"));
    completeTask(c1, w1, verdict.task_id, "裁决结论");

    // pipeline 自定义步骤：{{prev}} 注入上一步输出
    const run2 = (await rpc<proto.GroupRunResult>(userConn, "gr2", proto.METHOD_GROUP_RUN, {
      group_id: groupID,
      steps: [
        { run: w1, content: "第一步：列出要点" },
        { run: w2, content: "第二步：基于以下要点做汇总：\n{{prev}}" },
      ],
    } satisfies proto.GroupRunParams));
    void run2;
    const p1 = proto.decodeParams<proto.AgentChatParams>(await c1.next(proto.METHOD_AGENT_CHAT));
    completeTask(c1, w1, p1.task_id, "要点一二三");
    const p2 = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    assert.ok(p2.content?.includes("要点一二三"), "第二步应注入第一步输出");

    // 失败中止：超时步骤后不再派发后续步骤
    const run3 = (await rpc<proto.GroupRunResult>(userConn, "gr3", proto.METHOD_GROUP_RUN, {
      group_id: groupID,
      steps: [
        { run: w2, content: "永远不回", timeout_ms: 1000 },
        { run: w1, content: "不应执行" },
      ],
    } satisfies proto.GroupRunParams));
    void run3;
    const stuck = proto.decodeParams<proto.AgentChatParams>(await c2.next(proto.METHOD_AGENT_CHAT));
    const cancel = proto.decodeParams<proto.AgentCancelParams>(await c2.next(proto.METHOD_AGENT_CANCEL, 10_000));
    assert.equal(cancel.task_id, stuck.task_id);
    await expectSilence(c1, 1500); // 第二步未派发
  } finally {
    for (const c of conns) c.close();
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await fx.close();
  }
});
