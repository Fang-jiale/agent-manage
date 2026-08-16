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
import { setLogLevel } from "../src/util.ts";
import { signJwt, hashPassword } from "../src/auth.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "admin-test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
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
    instanceID: "admin-test",
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

// Conn buffers incoming messages from the moment the socket opens so that
// early server pushes are never lost.
class Conn {
  private ws: WebSocket;
  private buffer: proto.Message[] = [];
  private waiters: { method?: string; resolve: (m: proto.Message) => void; timer: NodeJS.Timeout }[] = [];
  closed: Promise<{ code: number }>;

  private constructor(ws: WebSocket) {
    this.ws = ws;
    this.closed = new Promise((resolve) => ws.once("close", (code) => resolve({ code })));
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

  // rpc sends a request and waits for the matching response by id.
  async rpc(id: string, method: string, params: object): Promise<proto.Message> {
    this.send(proto.newRequest(id, method, params));
    for (;;) {
      const m = await this.next();
      if (m.id === id) return m;
    }
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

function expectDial401(url: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once("open", () => reject(new Error("unexpected open")));
    ws.once("error", (err) => {
      if (String(err).includes("401")) resolve();
      else reject(err);
    });
  });
}

interface Fixture {
  base: string;
  db: Db;
  adminID: string;
  aliceID: string;
  bobID: string;
  close: () => Promise<void>;
}

async function setup(t: import("node:test").TestContext): Promise<Fixture | undefined> {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过管理后台测试");
    await db.close().catch(() => {});
    return undefined;
  }
  const suffix = crypto.randomUUID();
  const adminID = `adm-${suffix}`;
  const aliceID = `alice-${suffix}`;
  const bobID = `bob-${suffix}`;
  await db.createUser({ id: adminID, name: adminID, password_hash: hashPassword("pw"), role: "admin" });
  await db.createUser({ id: aliceID, name: aliceID, password_hash: hashPassword("pw") });
  await db.createUser({ id: bobID, name: bobID, password_hash: hashPassword("pw") });

  // 品牌目录非空会开启治理模式导致注册被拒：快照后清空（开放模式），结束后恢复
  const savedBrands = await db.listBrands();
  for (const b of savedBrands) await db.deleteBrand(b.id).catch(() => {});

  const { server } = await createGatewayServer(testConfig(), STATIC_FILE, db);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    base: `ws://localhost:${port}`,
    db, adminID, aliceID, bobID,
    close: async () => {
      await new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      });
      for (const b of savedBrands) {
        await db.createBrand({
          id: b.id, name: b.name, description: b.description, logo_url: b.logo_url,
          capabilities: b.capabilities, launch_cmd: b.launch_cmd, conn_type: b.conn_type, endpoint: b.endpoint,
        }).catch(() => {});
      }
      await db.deleteUser(adminID).catch(() => {});
      await db.deleteUser(aliceID).catch(() => {});
      await db.deleteUser(bobID).catch(() => {});
      await db.close();
    },
  };
}

async function registerAgent(conn: Conn, agentID: string): Promise<void> {
  const resp = await conn.rpc("reg-1", proto.METHOD_REGISTER, {
    agent_id: agentID,
    name: agentID,
    capabilities: [{ type: "chat", name: "general" }],
  } satisfies proto.RegisterParams);
  assert.equal(resp.error, undefined, `register error: ${JSON.stringify(resp.error)}`);
}

test("admin sees all agents; non-admin rejected from admin RPCs", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-shared");

    // bob 也有一个 agent，用于验证非 admin 的可见性隔离
    const bobAgentConn = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.bobID)}`);
    conns.push(bobAgentConn);
    await registerAgent(bobAgentConn, "agent-bob");

    // admin 收到全量广播（含他人 agent）
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    const push = proto.decodeParams<proto.AgentListParams>(await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.ok(push.agents.some((a) => a.id === "agent-shared"));
    assert.ok(push.agents.some((a) => a.id === "agent-bob"));

    // agent.list：DB 分页 + 实时合并
    const listResp = await adminConn.rpc("l1", proto.METHOD_AGENT_LIST, { query: "agent-shared" } satisfies proto.AdminAgentListParams);
    assert.equal(listResp.error, undefined);
    const list = listResp.result as proto.AdminAgentListResult;
    assert.equal(list.total, 1);
    assert.equal(list.agents[0].id, "agent-shared");
    assert.equal(list.agents[0].owner_id, fx.aliceID);
    assert.equal(list.agents[0].online, true);

    // 非 admin 的初始推送只含自己的 agent
    const aliceConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.aliceID)}`);
    conns.push(aliceConn);
    const alicePush = proto.decodeParams<proto.AgentListParams>(await aliceConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.ok(alicePush.agents.some((a) => a.id === "agent-shared"));
    assert.ok(!alicePush.agents.some((a) => a.id === "agent-bob"));

    // 非 admin 调 admin RPC → ERR_UNAUTHORIZED
    for (const [id, method, params] of [
      ["r2", proto.METHOD_AGENT_DISCONNECT, { agent_id: "agent-shared" }],
      ["r3", proto.METHOD_AGENT_REASSIGN, { agent_id: "agent-shared", owner_id: fx.bobID }],
      ["r4", proto.METHOD_ADMIN_OVERVIEW, {}],
      ["r5", proto.METHOD_USER_SET_ROLE, { id: fx.bobID, role: "admin" }],
    ] as const) {
      const resp = await aliceConn.rpc(id, method, params);
      assert.equal(resp.error?.code, proto.ERR_UNAUTHORIZED, method);
    }

    // agent.list 非 admin 已开放（owner-scoped）：只见自己的 agent
    const scoped = await aliceConn.rpc("r6", proto.METHOD_AGENT_LIST, {} satisfies proto.AdminAgentListParams);
    assert.equal(scoped.error, undefined);
    const scopedList = scoped.result as proto.AdminAgentListResult;
    assert.ok(scopedList.agents.some((a) => a.id === "agent-shared"));
    assert.ok(!scopedList.agents.some((a) => a.id === "agent-bob"));
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("cross-user task.create by admin delivers progress to admin", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const agentConn = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-x");

    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const taskID = `task-${crypto.randomUUID()}`;
    adminConn.send(proto.newRequest("req-1", proto.METHOD_TASK_CREATE, {
      agent_id: "agent-x",
      task_id: taskID,
      type: "chat",
      content: "hi from admin",
    } satisfies proto.TaskCreateParams));

    const chatReq = await agentConn.next(proto.METHOD_AGENT_CHAT);
    assert.equal(chatReq.id, "req-1");
    agentConn.send(proto.newResponse("req-1", {
      status: "accepted", task_id: taskID,
    } satisfies proto.TaskAcceptResult));
    const acceptResp = await adminConn.next();
    assert.equal(acceptResp.id, "req-1");

    agentConn.send(proto.newNotification(proto.METHOD_PROGRESS, {
      token: taskID,
      value: {
        kind: proto.PROGRESS_KIND_END,
        type: proto.CHUNK_TYPE_TEXT,
        agent_id: "agent-x",
        task_id: taskID,
        content: proto.textContent("done"),
        done: true,
      },
    } satisfies proto.ProgressParams));

    // admin（任务发起者，非 agent 属主）也能收到进度
    const progress = await adminConn.next(proto.METHOD_ADMIN_PROGRESS);
    const p = proto.decodeParams<proto.AdminProgressParams>(progress);
    assert.equal(p.task_id, taskID);
    assert.equal(p.done, true);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("user.set_role kicks target and takes effect on reconnect", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const bobConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.bobID)}`);
    conns.push(bobConn);
    await bobConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 不能改自己的角色
    const selfResp = await adminConn.rpc("s1", proto.METHOD_USER_SET_ROLE, {
      id: fx.adminID, role: "user",
    } satisfies proto.UserSetRoleParams);
    assert.equal(selfResp.error?.code, proto.ERR_INVALID_PARAMS);

    const okResp = await adminConn.rpc("s2", proto.METHOD_USER_SET_ROLE, {
      id: fx.bobID, role: "admin",
    } satisfies proto.UserSetRoleParams);
    assert.equal(okResp.error, undefined);

    // bob 被踢（角色缓存在连接上，重连后生效）
    const closed = await bobConn.closed;
    assert.equal(closed.code, 4001);

    const bobConn2 = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.bobID)}`);
    conns.push(bobConn2);
    await bobConn2.next(proto.METHOD_ADMIN_AGENT_LIST);
    const resp = await bobConn2.rpc("s3", proto.METHOD_ADMIN_OVERVIEW, {});
    assert.equal(resp.error, undefined, "bob should be admin after reconnect");
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("agent.disconnect and agent.reassign", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const agentConn = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-r");
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // reassign 到 bob：DB 与在线连接同步更新
    const reResp = await adminConn.rpc("ra1", proto.METHOD_AGENT_REASSIGN, {
      agent_id: "agent-r", owner_id: fx.bobID,
    } satisfies proto.AgentReassignParams);
    assert.equal(reResp.error, undefined);
    const row = (await fx.db.listAgentsPaged({ query: "agent-r", limit: 1, offset: 0 })).agents[0];
    assert.equal(row.owner_id, fx.bobID);
    const push = proto.decodeParams<proto.AgentListParams>(await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(push.agents.find((a) => a.id === "agent-r")?.owner_id, fx.bobID);

    const badResp = await adminConn.rpc("ra2", proto.METHOD_AGENT_REASSIGN, {
      agent_id: "agent-r", owner_id: "nonexistent-user",
    } satisfies proto.AgentReassignParams);
    assert.equal(badResp.error?.code, proto.ERR_INVALID_PARAMS);

    // disconnect 关闭 agent 连接
    const dcResp = await adminConn.rpc("d1", proto.METHOD_AGENT_DISCONNECT, {
      agent_id: "agent-r",
    } satisfies proto.AgentDisconnectParams);
    assert.equal(dcResp.error, undefined);
    const closed = await agentConn.closed;
    assert.equal(closed.code, 4001);

    const missResp = await adminConn.rpc("d2", proto.METHOD_AGENT_DISCONNECT, {
      agent_id: "agent-r",
    } satisfies proto.AgentDisconnectParams);
    assert.equal(missResp.error?.code, proto.ERR_AGENT_NOT_FOUND);

    // 离线后 DB 行保留、online=false
    const offResp = await adminConn.rpc("l1", proto.METHOD_AGENT_LIST, { query: "agent-r" } satisfies proto.AdminAgentListParams);
    const offList = offResp.result as proto.AdminAgentListResult;
    assert.equal(offList.total, 1);
    assert.equal(offList.agents[0].online, false);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("admin.overview returns counts", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const resp = await adminConn.rpc("o1", proto.METHOD_ADMIN_OVERVIEW, {});
    assert.equal(resp.error, undefined);
    const ov = resp.result as proto.OverviewResult;
    assert.ok(ov.users_total >= 3);
    assert.ok(ov.users_connected >= 1);
    assert.equal(ov.agents_online, 0);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("device key full lifecycle", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const aliceConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.aliceID)}`);
    conns.push(aliceConn);
    await aliceConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 创建（本人即可）
    const createResp = await aliceConn.rpc("k1", proto.METHOD_DEVICE_KEY_CREATE, {
      name: "ci-runner",
    } satisfies proto.DeviceKeyCreateParams);
    assert.equal(createResp.error, undefined);
    const created = createResp.result as proto.DeviceKeyCreateResult;
    assert.ok(created.key.startsWith("amk_"));

    // 用密钥连接 /ws/agent，归属 = 密钥属主
    const agentConn = await Conn.dial(`${fx.base}/ws/agent?key=${created.key}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-keyed");
    const alicePush = proto.decodeParams<proto.AgentListParams>(await aliceConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.ok(alicePush.agents.some((a) => a.id === "agent-keyed" && a.owner_id === fx.aliceID));

    // 列表：不返回明文/哈希，last_used_at 已记账
    const listResp = await aliceConn.rpc("k2", proto.METHOD_DEVICE_KEY_LIST, {} satisfies proto.DeviceKeyListParams);
    const keys = (listResp.result as proto.DeviceKeyListResult).keys;
    const mine = keys.find((k) => k.id === created.id);
    assert.ok(mine);
    assert.equal(mine.disabled, false);
    assert.ok(mine.last_used_at !== null);
    assert.equal((mine as unknown as Record<string, unknown>).key_hash, undefined);

    // 吊销 → 在线连接被踢，重连 401
    const revResp = await aliceConn.rpc("k3", proto.METHOD_DEVICE_KEY_REVOKE, {
      id: created.id,
    } satisfies proto.DeviceKeyRevokeParams);
    assert.equal(revResp.error, undefined);
    const closed = await agentConn.closed;
    assert.equal(closed.code, 4001);
    await expectDial401(`${fx.base}/ws/agent?key=${created.key}`);

    // 未知密钥同样裸 401
    await expectDial401(`${fx.base}/ws/agent?key=amk_deadbeefdeadbeefdeadbeef`);

    // /ws/admin 不接受设备密钥
    await expectDial401(`${fx.base}/ws/admin?key=${created.key}`);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("admin can create key for another user; non-admin cannot", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    const resp = await adminConn.rpc("k1", proto.METHOD_DEVICE_KEY_CREATE, {
      name: "for-alice", owner_id: fx.aliceID,
    } satisfies proto.DeviceKeyCreateParams);
    assert.equal(resp.error, undefined);
    const created = resp.result as proto.DeviceKeyCreateResult;

    const agentConn = await Conn.dial(`${fx.base}/ws/agent?key=${created.key}`);
    conns.push(agentConn);
    await registerAgent(agentConn, "agent-for-alice");
    const push = proto.decodeParams<proto.AgentListParams>(await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST));
    assert.equal(push.agents.find((a) => a.id === "agent-for-alice")?.owner_id, fx.aliceID);

    // 非 admin 代他人创建 → ERR_UNAUTHORIZED
    const bobConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.bobID)}`);
    conns.push(bobConn);
    await bobConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    const denied = await bobConn.rpc("k2", proto.METHOD_DEVICE_KEY_CREATE, {
      name: "sneaky", owner_id: fx.aliceID,
    } satisfies proto.DeviceKeyCreateParams);
    assert.equal(denied.error?.code, proto.ERR_UNAUTHORIZED);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("user.create manual id and user.delete cascade", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const adminConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(adminConn);
    await adminConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 手动录入用户 ID：成功 + 重复 ID / 非法格式拒绝
    const victimID = "victim-" + crypto.randomUUID().slice(0, 8);
    let r = await adminConn.rpc("uc-1", proto.METHOD_USER_CREATE, {
      id: victimID, name: victimID, password: "pw123456",
    } satisfies proto.UserCreateParams);
    assert.equal(r.error, undefined, JSON.stringify(r.error));
    assert.equal((r.result as proto.UserInfo).id, victimID);

    r = await adminConn.rpc("uc-2", proto.METHOD_USER_CREATE, {
      id: victimID, name: "dup-" + victimID, password: "pw123456",
    } satisfies proto.UserCreateParams);
    assert.equal(r.error?.code, proto.ERR_INVALID_PARAMS);

    r = await adminConn.rpc("uc-3", proto.METHOD_USER_CREATE, {
      id: "bad id!", name: "bad-" + victimID, password: "pw123456",
    } satisfies proto.UserCreateParams);
    assert.equal(r.error?.code, proto.ERR_INVALID_PARAMS);

    // 给 victim 铺数据：agent 行、会话+消息、设备密钥、备注名、群组
    const agentID = "del-agent-" + victimID;
    await fx.db.upsertAgent({
      id: agentID, owner_id: victimID, name: agentID,
      platform: null, capabilities: JSON.stringify([{ type: "chat", name: "general" }]), status: "offline",
    });
    const session = await fx.db.createSession({ id: "del-sess-" + victimID, owner_id: victimID, agent_id: agentID, title: "s" });
    await fx.db.createDeviceKey({ id: "del-key-" + victimID, owner_id: victimID, name: "k", key_hash: "hash-" + victimID });
    await fx.db.setNickname(victimID, agentID, "备注");
    await fx.db.createGroup({ id: "del-grp-" + victimID, owner_id: victimID, name: "g", manager_agent_id: null });
    await fx.db.addGroupMember("del-grp-" + victimID, agentID);

    // 非 admin 删除 → 拒绝；admin 删自己 → 拒绝
    const aliceConn = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.aliceID)}`);
    conns.push(aliceConn);
    await aliceConn.next(proto.METHOD_ADMIN_AGENT_LIST);
    let d = await aliceConn.rpc("ud-0", proto.METHOD_USER_DELETE, { id: victimID } satisfies proto.UserDeleteParams);
    assert.equal(d.error?.code, proto.ERR_UNAUTHORIZED);
    d = await adminConn.rpc("ud-self", proto.METHOD_USER_DELETE, { id: fx.adminID } satisfies proto.UserDeleteParams);
    assert.equal(d.error?.code, proto.ERR_INVALID_PARAMS);

    // admin 删除 → 连带清理
    d = await adminConn.rpc("ud-1", proto.METHOD_USER_DELETE, { id: victimID } satisfies proto.UserDeleteParams);
    assert.equal(d.error, undefined, JSON.stringify(d.error));

    assert.equal(await fx.db.getUserById(victimID), undefined);
    assert.equal((await fx.db.listAgentsPaged({ ownerID: victimID, limit: 10, offset: 0 })).agents.length, 0);
    assert.equal((await fx.db.listSessions(victimID)).length, 0);
    assert.equal((await fx.db.listDeviceKeys(victimID)).length, 0);
    assert.equal((await fx.db.listGroups(victimID)).length, 0);
    const nicks = await fx.db.listNicknamesForOwner(victimID);
    assert.equal(nicks.size, 0);
    assert.equal(session.id, "del-sess-" + victimID); // 使用返回值避免未消费告警
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});
