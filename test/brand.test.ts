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
const JWT_SECRET = "brand-test-secret";
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
    instanceID: "brand-test",
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

interface Fixture {
  base: string;
  db: Db;
  adminID: string;
  aliceID: string;
  close: () => Promise<void>;
}

async function setup(t: import("node:test").TestContext): Promise<Fixture | undefined> {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过品牌/审批测试");
    await db.close().catch(() => {});
    return undefined;
  }
  const suffix = crypto.randomUUID();
  const adminID = `adm-${suffix}`;
  const aliceID = `alice-${suffix}`;
  await db.createUser({ id: adminID, name: adminID, password_hash: hashPassword("pw"), role: "admin" });
  await db.createUser({ id: aliceID, name: aliceID, password_hash: hashPassword("pw") });

  const { server } = await createGatewayServer(testConfig(), STATIC_FILE, db);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  const brandIDs: string[] = [];
  return {
    base: `ws://localhost:${port}`,
    db, adminID, aliceID,
    close: async () => {
      await new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      });
      for (const id of brandIDs) await db.deleteBrand(id).catch(() => {});
      await db.deleteUser(adminID).catch(() => {});
      await db.deleteUser(aliceID).catch(() => {});
      await db.close();
    },
    // 测试内建品牌时登记 id 以便清理
    ...({ trackBrand: (id: string) => brandIDs.push(id) } as object),
  } as Fixture & { trackBrand(id: string): void };
}

async function createBrand(fx: Fixture & { trackBrand?(id: string): void }, admin: Conn, name: string): Promise<proto.BrandInfo> {
  const resp = await admin.rpc("bc-" + name, proto.METHOD_BRAND_CREATE, {
    name,
    description: "测试品牌",
    logo_url: "/files/attachments/x/logo.png",
    capabilities: [{ type: "chat", name: "coding", description: "品牌能力" }],
  } satisfies proto.BrandCreateParams);
  assert.equal(resp.error, undefined, `brand.create error: ${JSON.stringify(resp.error)}`);
  const brand = resp.result as proto.BrandInfo;
  fx.trackBrand?.(brand.id);
  return brand;
}

test("brand CRUD 与鉴权", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    const alice = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.aliceID)}`);
    conns.push(admin, alice);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    await alice.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 非 admin 不能建品牌
    const denied = await alice.rpc("b0", proto.METHOD_BRAND_CREATE, { name: "x-brand" } satisfies proto.BrandCreateParams);
    assert.equal(denied.error?.code, proto.ERR_UNAUTHORIZED);

    const brand = await createBrand(fx, admin, `brand-${crypto.randomUUID().slice(0, 8)}`);
    assert.equal(brand.description, "测试品牌");

    // 重名拒绝
    const dup = await admin.rpc("b1", proto.METHOD_BRAND_CREATE, { name: brand.name } satisfies proto.BrandCreateParams);
    assert.equal(dup.error?.code, proto.ERR_INVALID_PARAMS);

    // 列表可见
    const list = await admin.rpc("b2", proto.METHOD_BRAND_LIST, {});
    const brands = (list.result as proto.BrandListResult).brands;
    assert.ok(brands.some((b) => b.id === brand.id));

    // 更新（禁用）
    const upd = await admin.rpc("b3", proto.METHOD_BRAND_UPDATE, {
      id: brand.id, name: brand.name, disabled: true,
    } satisfies proto.BrandUpdateParams);
    assert.equal(upd.error, undefined);
    const list2 = await admin.rpc("b4", proto.METHOD_BRAND_LIST, {});
    const after = (list2.result as proto.BrandListResult).brands.find((b) => b.id === brand.id);
    assert.equal(after?.disabled, true);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("治理模式：无品牌注册被拒，带品牌注册被覆盖名称/能力，主动注册进待审批", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(admin);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const brand = await createBrand(fx, admin, `brand-${crypto.randomUUID().slice(0, 8)}`);
    // 品牌广播可能先行到达，不影响后续断言

    // 1. 不带 brand_id → 拒绝并关连接
    const noBrand = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(noBrand);
    const r1 = await noBrand.rpc("r1", proto.METHOD_REGISTER, {
      agent_id: "free-agent", name: "free-agent", capabilities: [],
    } satisfies proto.RegisterParams);
    assert.equal(r1.error?.code, proto.ERR_INVALID_PARAMS);
    assert.match(r1.error?.message ?? "", /brand_id required/);
    assert.equal((await noBrand.closed).code, 4001);

    // 2. 品牌不存在 → 拒绝
    const badBrand = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(badBrand);
    const r2 = await badBrand.rpc("r2", proto.METHOD_REGISTER, {
      agent_id: "free-agent-2", name: "x", capabilities: [], brand_id: "nonexistent",
    } satisfies proto.RegisterParams);
    assert.equal(r2.error?.code, proto.ERR_INVALID_PARAMS);

    // 3. 带合法品牌：注册成功但进待审批；名称/能力被品牌覆盖
    const good = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(good);
    const agentID = `self-${crypto.randomUUID().slice(0, 8)}`;
    const r3 = await good.rpc("r3", proto.METHOD_REGISTER, {
      agent_id: agentID, name: "伪造的名字", capabilities: [{ type: "x", name: "fake" }], brand_id: brand.id,
    } satisfies proto.RegisterParams);
    assert.equal(r3.error, undefined, `register error: ${JSON.stringify(r3.error)}`);

    const listMsg = await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const info = (proto.decodeParams<proto.AgentListParams>(listMsg).agents).find((a) => a.id === agentID);
    assert.ok(info, "agent should appear in list");
    assert.equal(info.name, brand.name); // 名称被品牌覆盖
    assert.equal(info.capabilities[0]?.name, "coding"); // 能力被品牌覆盖
    assert.equal(info.logo_url, "/files/attachments/x/logo.png");
    assert.equal(info.approval_status, "pending");
    assert.equal(info.status, "pending");

    // 4. 待审批 agent 不接任务
    const tc = await admin.rpc("t1", proto.METHOD_TASK_CREATE, {
      agent_id: agentID, task_id: "task-pend", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams);
    assert.equal(tc.error?.code, proto.ERR_UNAUTHORIZED);
    assert.match(tc.error?.message ?? "", /pending approval/);

    // 5. 批准后可用：task.create 不再被挡，agent 连接收到 agent.chat
    const ap = await admin.rpc("a1", proto.METHOD_AGENT_APPROVE, { agent_id: agentID } satisfies proto.AgentApprovalParams);
    assert.equal(ap.error, undefined);
    admin.send(proto.newRequest("t2", proto.METHOD_TASK_CREATE, {
      agent_id: agentID, task_id: "task-ok", type: "chat", content: "hi",
    } satisfies proto.TaskCreateParams));
    const chat = await good.next(proto.METHOD_AGENT_CHAT);
    assert.equal(proto.decodeParams<proto.AgentChatParams>(chat).agent_id, agentID); // 网关注入 agent_id

    // 6. 拒绝后再注册被拒
    const agentID2 = `self-${crypto.randomUUID().slice(0, 8)}`;
    const conn2 = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(conn2);
    await conn2.rpc("r4", proto.METHOD_REGISTER, {
      agent_id: agentID2, capabilities: [], brand_id: brand.id,
    } satisfies proto.RegisterParams);
    const rej = await admin.rpc("a2", proto.METHOD_AGENT_REJECT, { agent_id: agentID2 } satisfies proto.AgentApprovalParams);
    assert.equal(rej.error, undefined);
    assert.equal((await conn2.closed).code, 4001);

    const conn3 = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(conn3);
    const r5 = await conn3.rpc("r5", proto.METHOD_REGISTER, {
      agent_id: agentID2, capabilities: [], brand_id: brand.id,
    } satisfies proto.RegisterParams);
    assert.equal(r5.error?.code, proto.ERR_UNAUTHORIZED);
    assert.match(r5.error?.message ?? "", /rejected/);

    // 清理审批产生的 agents 行
    await fx.db.unassignAgent(agentID).catch(() => {});
    await fx.db.unassignAgent(agentID2).catch(() => {});
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("connector：hello → assign → sync → 注册即用（免审批）→ remove 对账下线", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(admin);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const brand = await createBrand(fx, admin, `brand-${crypto.randomUUID().slice(0, 8)}`);

    // connector 上线
    const connector = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(connector);
    const connectorID = `conn-${crypto.randomUUID().slice(0, 8)}`;
    const hello = await connector.rpc("h1", proto.METHOD_CONNECTOR_HELLO, {
      connector_id: connectorID,
      platform: { os: "darwin", arch: "arm64" },
    } satisfies proto.ConnectorHelloParams);
    assert.equal(hello.error, undefined);
    // hello 后会立即推一次全量 sync（此时为空集），先消费掉
    const sync0 = await connector.next(proto.METHOD_CONNECTOR_SYNC);
    assert.equal(proto.decodeParams<proto.ConnectorSyncParams>(sync0).agents.length, 0);

    // connector.list 可见（属主是 alice）
    const cl = await admin.rpc("c1", proto.METHOD_CONNECTOR_LIST, {});
    const found = (cl.result as proto.ConnectorListResult).connectors.find((c) => c.id === connectorID);
    assert.ok(found, "connector should be listed");
    assert.equal(found.owner_id, fx.aliceID);

    // 分配 agent（admin 操作 alice 的 connector）
    const asg = await admin.rpc("as1", proto.METHOD_AGENT_ASSIGN, {
      connector_id: connectorID, brand_id: brand.id,
    } satisfies proto.AgentAssignParams);
    assert.equal(asg.error, undefined, `assign error: ${JSON.stringify(asg.error)}`);
    const agentID = (asg.result as proto.AgentAssignResult).agent_id;
    assert.ok(agentID.startsWith(brand.name.replace(/[^\w-]/g, "-") + "-"));

    // connector 收到全量 sync
    const sync = await connector.next(proto.METHOD_CONNECTOR_SYNC);
    const syncAgents = proto.decodeParams<proto.ConnectorSyncParams>(sync).agents;
    assert.equal(syncAgents.length, 1);
    assert.equal(syncAgents[0].agent_id, agentID);
    assert.equal(syncAgents[0].capabilities[0]?.name, "coding");

    // connector 按 sync 注册：已批准，立即可用
    const reg = await connector.rpc("reg-1", proto.METHOD_REGISTER, {
      agent_id: agentID, name: agentID, capabilities: syncAgents[0].capabilities, brand_id: brand.id,
    } satisfies proto.RegisterParams);
    assert.equal(reg.error, undefined);

    const listMsg = await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const info = proto.decodeParams<proto.AgentListParams>(listMsg).agents.find((a) => a.id === agentID);
    assert.equal(info?.approval_status, "approved");
    assert.equal(info?.status, "online");

    // 任务直达，agent_id 注入
    admin.send(proto.newRequest("t1", proto.METHOD_TASK_CREATE, {
      agent_id: agentID, task_id: "task-conn", type: "chat", content: "hello",
    } satisfies proto.TaskCreateParams));
    const chat = await connector.next(proto.METHOD_AGENT_CHAT);
    const chatParams = proto.decodeParams<proto.AgentChatParams>(chat);
    assert.equal(chatParams.agent_id, agentID);
    assert.equal(chatParams.content, "hello");

    // 移除：sync 对账为空；agent 从列表消失（ws 保持——同连接其他 agent 不受影响）
    const rm = await admin.rpc("rm1", proto.METHOD_AGENT_REMOVE, { agent_id: agentID } satisfies proto.AgentRemoveParams);
    assert.equal(rm.error, undefined);
    const sync2 = await connector.next(proto.METHOD_CONNECTOR_SYNC);
    assert.equal(proto.decodeParams<proto.ConnectorSyncParams>(sync2).agents.length, 0);
    const listMsg2 = await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    assert.ok(!proto.decodeParams<proto.AgentListParams>(listMsg2).agents.some((a) => a.id === agentID));
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("配对接入：配对码绑 owner → pair 挂起 → 批准发密钥 → sync 带 launch_cmd", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  let codeID = "";
  let brandID = "";
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    const alice = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.aliceID)}`);
    conns.push(admin, alice);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    await alice.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 品牌带 launch_cmd + 连接方式/服务地址
    const launchCmd = "node package/dist/ywmatrix-shim.mjs --workdir /tmp";
    const bc = await admin.rpc("bc-l", proto.METHOD_BRAND_CREATE, {
      name: `brand-${crypto.randomUUID().slice(0, 8)}`,
      capabilities: [{ type: "chat", name: "coding" }],
      conn_type: "http",
      launch_cmd: launchCmd,
      endpoint: "http://127.0.0.1:9001",
    } satisfies proto.BrandCreateParams);
    assert.equal(bc.error, undefined);
    const brand = bc.result as proto.BrandInfo;
    brandID = brand.id;
    assert.equal(brand.launch_cmd, launchCmd);
    assert.equal(brand.conn_type, "http");
    assert.equal(brand.endpoint, "http://127.0.0.1:9001");

    // alice 生成配对码：owner 即 alice
    const pc = await alice.rpc("p1", proto.METHOD_PAIRING_CREATE, {} satisfies proto.PairingCodeCreateParams);
    assert.equal(pc.error, undefined);
    const created = pc.result as proto.PairingCodeCreateResult;
    assert.equal(created.owner_id, fx.aliceID);
    assert.ok(created.code.length > 8);
    codeID = created.id;

    // 非法码：报错并关连接
    const bad = await Conn.dial(`${fx.base}/ws/agent?pair=1`);
    conns.push(bad);
    const rBad = await bad.rpc("pb", proto.METHOD_CONNECTOR_PAIR, {
      code: "wrong-code", connector_id: "conn-bad",
    } satisfies proto.ConnectorPairParams);
    assert.equal(rBad.error?.code, proto.ERR_INVALID_PARAMS);
    assert.equal((await bad.closed).code, 4001);

    // 配对连接只允许 connector.pair
    const pairConn = await Conn.dial(`${fx.base}/ws/agent?pair=1`);
    conns.push(pairConn);
    const denied = await pairConn.rpc("hb", proto.METHOD_HEARTBEAT, { timestamp: proto.rfc3339Now() });
    assert.equal(denied.error?.code, proto.ERR_INVALID_REQUEST);

    // 合法码：受理为 pending
    const connectorID = `conn-${crypto.randomUUID().slice(0, 8)}`;
    const pr = await pairConn.rpc("pp", proto.METHOD_CONNECTOR_PAIR, {
      code: created.code, connector_id: connectorID,
      platform: { os: "darwin", arch: "arm64" },
    } satisfies proto.ConnectorPairParams);
    assert.equal(pr.error, undefined);
    assert.equal((pr.result as proto.ConnectorPairResult).status, "pending");

    // 待接入列表可见，owner 来自配对码
    const pl = await admin.rpc("pl1", proto.METHOD_CONNECTOR_PENDING_LIST, {});
    const pending = (pl.result as proto.ConnectorPendingListResult).connectors.find((c) => c.connector_id === connectorID);
    assert.ok(pending, "pending connector should be listed");
    assert.equal(pending.owner_id, fx.aliceID);

    // 批准：connector 收到凭证推送
    const ap = await admin.rpc("pa", proto.METHOD_CONNECTOR_APPROVE, { connector_id: connectorID } satisfies proto.ConnectorApproveParams);
    assert.equal(ap.error, undefined);
    const cred = await pairConn.next(proto.METHOD_CONNECTOR_CREDENTIAL);
    const credParams = proto.decodeParams<proto.ConnectorCredentialParams>(cred);
    assert.equal(credParams.connector_id, connectorID);
    assert.ok(credParams.key.length > 8);

    // 配对码已消耗：再次 pair 同码被拒
    const reuse = await Conn.dial(`${fx.base}/ws/agent?pair=1`);
    conns.push(reuse);
    const rReuse = await reuse.rpc("pr2", proto.METHOD_CONNECTOR_PAIR, {
      code: created.code, connector_id: `conn-${crypto.randomUUID().slice(0, 8)}`,
    } satisfies proto.ConnectorPairParams);
    assert.equal(rReuse.error?.code, proto.ERR_INVALID_PARAMS);

    // 凭密钥重连走 hello：属主是 alice（密钥 owner = 配对码 owner）
    const connector = await Conn.dial(`${fx.base}/ws/agent?key=${encodeURIComponent(credParams.key)}`);
    conns.push(connector);
    const hello = await connector.rpc("h1", proto.METHOD_CONNECTOR_HELLO, { connector_id: connectorID } satisfies proto.ConnectorHelloParams);
    assert.equal(hello.error, undefined);
    await connector.next(proto.METHOD_CONNECTOR_SYNC); // 空集
    const cl = await admin.rpc("c1", proto.METHOD_CONNECTOR_LIST, {});
    assert.equal((cl.result as proto.ConnectorListResult).connectors.find((c) => c.id === connectorID)?.owner_id, fx.aliceID);

    // 分配该品牌 agent：sync 下发 launch_cmd
    const asg = await admin.rpc("as1", proto.METHOD_AGENT_ASSIGN, {
      connector_id: connectorID, brand_id: brand.id,
    } satisfies proto.AgentAssignParams);
    assert.equal(asg.error, undefined);
    const sync = await connector.next(proto.METHOD_CONNECTOR_SYNC);
    const syncAgents = proto.decodeParams<proto.ConnectorSyncParams>(sync).agents;
    assert.equal(syncAgents.length, 1);
    assert.equal(syncAgents[0].launch_cmd, launchCmd);
    assert.equal(syncAgents[0].conn_type, "http");
    assert.equal(syncAgents[0].endpoint, "http://127.0.0.1:9001");
    await fx.db.unassignAgent(syncAgents[0].agent_id).catch(() => {});
  } finally {
    for (const c of conns) c.close();
    if (brandID) await fx?.db.deleteBrand(brandID).catch(() => {});
    if (codeID) await fx?.db.deletePairingCode(codeID).catch(() => {});
    await fx?.close();
  }
});

test("connector 自助通道：brand.list / agent.assign / agent.remove 限本 connector", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(admin);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const brand = await createBrand(fx, admin, `brand-${crypto.randomUUID().slice(0, 8)}`);

    // 普通 agent 连接（未 hello 不是 connector）：自助 RPC 被拒
    const plain = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(plain);
    const denied0 = await plain.rpc("d0", proto.METHOD_BRAND_LIST, {});
    assert.equal(denied0.error?.code, proto.ERR_UNAUTHORIZED);
    plain.close();

    // 两个 connector 上线
    const connA = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    const connB = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(connA, connB);
    const idA = `conn-a-${crypto.randomUUID().slice(0, 6)}`;
    const idB = `conn-b-${crypto.randomUUID().slice(0, 6)}`;
    await connA.rpc("ha", proto.METHOD_CONNECTOR_HELLO, { connector_id: idA } satisfies proto.ConnectorHelloParams);
    await connA.next(proto.METHOD_CONNECTOR_SYNC);
    await connB.rpc("hb", proto.METHOD_CONNECTOR_HELLO, { connector_id: idB } satisfies proto.ConnectorHelloParams);
    await connB.next(proto.METHOD_CONNECTOR_SYNC);

    // connector 可列品牌（只读目录）
    const bl = await connA.rpc("bl", proto.METHOD_BRAND_LIST, {});
    assert.equal(bl.error, undefined);
    assert.ok((bl.result as proto.BrandListResult).brands.some((b) => b.id === brand.id));

    // 不能给别人的 connector 分配
    const cross = await connA.rpc("x1", proto.METHOD_AGENT_ASSIGN, {
      connector_id: idB, brand_id: brand.id,
    } satisfies proto.AgentAssignParams);
    assert.equal(cross.error?.code, proto.ERR_UNAUTHORIZED);

    // 给自己分配：成功并收到 sync（响应与 sync 顺序不定，一起等）
    connA.send(proto.newRequest("a1", proto.METHOD_AGENT_ASSIGN, {
      connector_id: idA, brand_id: brand.id,
    } satisfies proto.AgentAssignParams));
    let agentID = "";
    let syncAgents: proto.ConnectorSyncAgent[] = [];
    for (let i = 0; i < 2; i++) {
      const m = await connA.next();
      if (m.id === "a1") {
        assert.equal(m.error, undefined, `assign error: ${JSON.stringify(m.error)}`);
        agentID = (m.result as proto.AgentAssignResult).agent_id;
      } else if (m.method === proto.METHOD_CONNECTOR_SYNC) {
        syncAgents = proto.decodeParams<proto.ConnectorSyncParams>(m).agents;
      }
    }
    assert.ok(agentID !== "");
    assert.ok(syncAgents.some((a) => a.agent_id === agentID));

    // 不能移除别人托管的 agent
    const rmCross = await connB.rpc("rx", proto.METHOD_AGENT_REMOVE, { agent_id: agentID } satisfies proto.AgentRemoveParams);
    assert.equal(rmCross.error?.code, proto.ERR_INVALID_PARAMS);

    // 自己移除：成功，sync 对账为空（同样响应与 sync 一起等）
    connA.send(proto.newRequest("r1", proto.METHOD_AGENT_REMOVE, { agent_id: agentID } satisfies proto.AgentRemoveParams));
    let removed = false;
    let emptied = false;
    for (let i = 0; i < 2; i++) {
      const m = await connA.next();
      if (m.id === "r1") {
        assert.equal(m.error, undefined);
        removed = true;
      } else if (m.method === proto.METHOD_CONNECTOR_SYNC) {
        emptied = proto.decodeParams<proto.ConnectorSyncParams>(m).agents.length === 0;
      }
    }
    assert.ok(removed && emptied);
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("connector 重连：旧连接迟到的 close 不误删新连接的 connector 条目", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(admin);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const connectorID = `conn-${crypto.randomUUID().slice(0, 8)}`;

    const a = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(a);
    await a.rpc("ha", proto.METHOD_CONNECTOR_HELLO, { connector_id: connectorID } satisfies proto.ConnectorHelloParams);
    await a.next(proto.METHOD_CONNECTOR_SYNC);

    // 同 connector_id 重连（新连接接管）
    const b = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(b);
    await b.rpc("hb", proto.METHOD_CONNECTOR_HELLO, { connector_id: connectorID } satisfies proto.ConnectorHelloParams);
    await b.next(proto.METHOD_CONNECTOR_SYNC);

    // 旧连接迟到的 close（半连接/事件乱序）：不应删掉新连接的条目
    a.close();
    await new Promise((r) => setTimeout(r, 300));

    const cl = await admin.rpc("cl", proto.METHOD_CONNECTOR_LIST, {});
    assert.ok(
      (cl.result as proto.ConnectorListResult).connectors.some((c) => c.id === connectorID),
      "connector entry should survive stale close",
    );
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});

test("connector 双实例：后报到者踢掉旧连接（4002），条目归新连接", async (t) => {
  const fx = await setup(t);
  if (!fx) return;
  const conns: Conn[] = [];
  try {
    const connectorID = `conn-${crypto.randomUUID().slice(0, 8)}`;
    const a = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(a);
    await a.rpc("ha", proto.METHOD_CONNECTOR_HELLO, { connector_id: connectorID } satisfies proto.ConnectorHelloParams);
    await a.next(proto.METHOD_CONNECTOR_SYNC);

    const b = await Conn.dial(`${fx.base}/ws/agent?token=${jwtFor(fx.aliceID)}`);
    conns.push(b);
    await b.rpc("hb", proto.METHOD_CONNECTOR_HELLO, { connector_id: connectorID } satisfies proto.ConnectorHelloParams);
    await b.next(proto.METHOD_CONNECTOR_SYNC);

    // 旧连接被 4002 踢掉（client 收到 4002 应退出而非重连）
    const closed = await a.closed;
    assert.equal(closed.code, 4002);

    // connector 条目归新连接
    const admin = await Conn.dial(`${fx.base}/ws/admin?token=${jwtFor(fx.adminID)}`);
    conns.push(admin);
    await admin.next(proto.METHOD_ADMIN_AGENT_LIST);
    const cl = await admin.rpc("cl", proto.METHOD_CONNECTOR_LIST, {});
    assert.ok((cl.result as proto.ConnectorListResult).connectors.some((c) => c.id === connectorID));
  } finally {
    for (const c of conns) c.close();
    await fx.close();
  }
});
