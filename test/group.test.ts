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

setLogLevel("error");

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

    // 子任务不能再编排（depth=1 → -32006）；须在子任务完成（untrack）前发起
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
