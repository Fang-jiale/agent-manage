import http from "node:http";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";
import * as proto from "./protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, setLogLevel, logger, parseListenAddr } from "./util.ts";
import { Db, type DbUser, type DbAgent, type DbAgentBrand, type DbPairingCode } from "./db.ts";
import { Bus, type RegisteredAgent } from "./bus.ts";
import {
  type AttachmentStore,
  LocalAttachmentStore,
  createLocalAttachmentStore,
  createS3AttachmentStore,
  sanitizeFileName,
} from "./storage.ts";
import { hashPassword, verifyPassword, signJwt, verifyJwt } from "./auth.ts";
import { OIDCProvider } from "./oidc.ts";
import { Metrics } from "./metrics.ts";
import { RateLimiter, clientIp } from "./ratelimit.ts";

interface AgentConn {
  id: string;
  ownerID: string;
  name: string;
  ws: WebSocket;
  capabilities: proto.Capability[];
  platform?: proto.PlatformInfo;
  status: string;
  lastHeartbeat: number;
  alive: boolean;
  deviceKeyID?: string; // 设备密钥连接记录 key id，吊销时按此踢线
  lastDbTouch?: number; // agents 表状态落库节流用
  lastRegistryTouch?: number; // 注册表刷新节流用
  ip?: string; // AgentClient 连接远端 IP（X-Forwarded-For 优先，否则 socket 远端）
  brandID?: string; // 品牌目录 id（治理模式）
  connectorID?: string; // 承载该 agent 的 connector（页面分配的实例才有）
  approval?: string; // approved | pending；pending 可用连接但不可接任务
  pairing?: boolean; // 无凭证的配对连接（?pair=1），只允许 connector.pair
}

// 待接入 connector（内存态）：pair 受理后挂起，等管理员批准后下发设备密钥
interface PendingPair {
  connectorID: string;
  ownerID: string; // 配对码归属用户，批准后密钥归该用户
  codeID: string;
  conn: AgentConn;
  platform?: proto.PlatformInfo;
  version?: string;
  ip?: string;
  pairedAt: number;
}

interface UserConn {
  ws: WebSocket;
  userID: string;
  lastHeartbeat: number;
  alive: boolean;
  isAdmin: boolean; // WS 升级时缓存的角色；角色变更通过 kickUser 强制重连刷新
  ownOnly: boolean; // 对话页连接（?scope=own）：即使 admin 也只推送/通知自己名下的 agent
}

interface TaskState {
  agentID: string;
  ownerID: string;
  sessionID: string;
  timer: NodeJS.Timeout;
  createdAt: number;
  groupID?: string;       // 群聊任务：归属群
  parentTaskID?: string;  // 管理者编排：发起 invoke 的父任务
  invokerAgentID?: string; // 管理者编排：管理者 agent（子任务结束时回投结果）
  depth: number;          // 0 = 用户直发，1 = 管理者编排的子任务
}

// 群消息 fan-out 的单群目标 agent 上限
const MAX_GROUP_FANOUT = 8;

interface PendingEntry {
  user: UserConn;
  timer: NodeJS.Timeout; // agent 不应答时兜底，防泄漏
  taskID?: string; // agent.chat 转发携带：agent 以错误响应拒绝时用于清理任务条目
}

interface TaskBuffer {
  ownerID: string;
  agentID: string;
  sessionID: string;
  chunks: proto.LocalAgentChunk[];
  bytes: number;    // 已缓冲体积，配合 MAX_TASK_BUFFER_BYTES 防长任务撑爆内存
  truncated: boolean;
}

// 单任务落库缓冲上限：超出部分丢弃并在最终消息里标注截断
const MAX_TASK_BUFFER_BYTES = 4 * 1024 * 1024;

// 交互 chunk（confirm/prompt/block）在任务缓冲里的应答/撤销标记，随任务落库
interface BufferedInteractionChunk {
  type?: string;
  confirm_id?: string;
  prompt_id?: string;
  block_id?: string;
  answered?: boolean;
  answer?: unknown;
  cancelled?: boolean;
  reason?: string;
}

function isInteractionChunk(c: BufferedInteractionChunk): boolean {
  return c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED
    || c.type === proto.CHUNK_TYPE_PROMPT_REQUIRED
    || c.type === proto.CHUNK_TYPE_BLOCK_REQUIRED;
}

// respond 只带命中的 id；id 缺省（旧 client）时退化为同类型首个待决 chunk
function matchesInteractionChunk(
  c: BufferedInteractionChunk, confirmID: string, promptID: string, blockID: string,
): boolean {
  if (c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED) return confirmID === "" || c.confirm_id === confirmID;
  if (c.type === proto.CHUNK_TYPE_PROMPT_REQUIRED) return promptID === "" || c.prompt_id === promptID;
  return blockID === "" || c.block_id === blockID;
}

export class Hub {
  agents = new Map<string, AgentConn>();
  users = new Map<WebSocket, UserConn>();
  connectors = new Map<string, AgentConn>(); // connector 模式 client 连接（id = connector_id）
  pendingPairs = new Map<string, PendingPair>(); // 待审批的配对连接（id = connector_id）
  pendingRequests = new Map<string, PendingEntry>();
  tasks = new Map<string, TaskState>();
  taskBuffers = new Map<string, TaskBuffer>();
  db?: Db;
  bus?: Bus;
  attachments?: AttachmentStore;
  metrics = new Metrics();
  taskLimiter = new RateLimiter(30, 60_000); // 每用户每分钟 30 个任务
  deviceKeyLimiter = new RateLimiter(10, 60_000); // 每用户每分钟 10 次密钥创建
  draining = false;
  // 心跳超时探活：ws → ping 发出时刻。pong 处理器据此清标记并续命
  livenessProbes = new Map<WebSocket, number>();
  // 品牌目录缓存：启动时载入，CRUD 后 reload。空目录 = 开放模式（自由注册免审批）
  brands = new Map<string, DbAgentBrand>();

  private agentTimeoutMs: number;
  private userTimeoutMs: number;
  private taskTimeoutMs: number;
  private pendingTimeoutMs: number;
  private checker: NodeJS.Timeout;
  private agentListTimer?: NodeJS.Timeout; // broadcastAgentList 防抖

  constructor(agentTimeoutMs: number, userTimeoutMs: number, taskTimeoutMs: number, pendingTimeoutMs = 60_000) {
    this.agentTimeoutMs = agentTimeoutMs;
    this.userTimeoutMs = userTimeoutMs;
    this.taskTimeoutMs = taskTimeoutMs;
    this.pendingTimeoutMs = pendingTimeoutMs;
    this.checker = setInterval(() => this.heartbeatCheck(), 30_000);
    this.checker.unref();
    this.metrics.counter("ywm_tasks_created_total", "Tasks created by users");
    this.metrics.counter("ywm_tasks_completed_total", "Tasks finished with done=true");
    this.metrics.counter("ywm_tasks_failed_total", "Tasks finished with error");
    this.metrics.counter("ywm_tasks_timeout_total", "Tasks killed by timeout");
    this.metrics.counter("ywm_task_duration_seconds_sum", "Total task duration seconds");
    this.metrics.counter("ywm_task_duration_seconds_count", "Duration sample count");
    this.metrics.counter("ywm_messages_persisted_total", "Messages written to MySQL");
    this.metrics.counter("ywm_attachments_uploaded_total", "Attachment uploads");
    this.metrics.counter("ywm_attachment_bytes_total", "Attachment bytes uploaded");
  }

  // 任务终结时记录指标（done / error / timeout 三个出口都走这里）
  observeTaskEnd(taskID: string, outcome: "completed" | "failed" | "timeout"): void {
    const ts = this.tasks.get(taskID);
    this.metrics.inc(`ywm_tasks_${outcome}_total`);
    if (ts) {
      const secs = (Date.now() - ts.createdAt) / 1000;
      this.metrics.inc("ywm_task_duration_seconds_sum", secs);
      this.metrics.inc("ywm_task_duration_seconds_count");
    }
  }

  // 优雅关闭：停止心跳巡检与任务计时，落库缓冲消息，以 1001 断开所有 WS
  // （client 有重连逻辑，多实例下会自动接到其他实例）。DB 连接由调用方在
  // 短暂宽限后关闭，让 flush 的异步写入落地。
  shutdown(): void {
    this.draining = true;
    clearInterval(this.checker);
    if (this.agentListTimer) {
      clearTimeout(this.agentListTimer);
      this.agentListTimer = undefined;
    }
    for (const ts of this.tasks.values()) clearTimeout(ts.timer);
    for (const taskID of [...this.taskBuffers.keys()]) this.flushTaskBuffer(taskID);
    for (const [reqID, p] of this.pendingRequests) {
      clearTimeout(p.timer);
      try {
        p.user.ws.send(JSON.stringify(proto.newErrorResponse(reqID, proto.ERR_INTERNAL_ERROR, "server shutting down")));
      } catch { /* 连接可能已断开 */ }
    }
    this.pendingRequests.clear();
    for (const agent of this.agents.values()) agent.ws.close(1001, "server shutting down");
    for (const conn of this.connectors.values()) conn.ws.close(1001, "server shutting down");
    for (const p of this.pendingPairs.values()) p.conn.ws.close(1001, "server shutting down");
    for (const ws of this.users.keys()) ws.close(1001, "server shutting down");
  }

  private heartbeatCheck(): void {
    for (const [id, agent] of this.agents) {
      if (Date.now() - agent.lastHeartbeat > this.agentTimeoutMs) {
        // 唤醒竞态兜底：超时先 ping 探活，pong 会刷新 lastHeartbeat；下一轮仍超时才踢
        const probed = this.livenessProbes.get(agent.ws);
        if (probed === undefined) {
          this.livenessProbes.set(agent.ws, Date.now());
          try { agent.ws.ping(); } catch { /* socket 已坏，下轮关闭 */ }
          continue;
        }
        if (Date.now() - probed < 10_000) continue;
        logger.warn("agent heartbeat timeout", { agent_id: id });
        this.livenessProbes.delete(agent.ws);
        this.unregisterAgent(id);
        agent.ws.close();
      }
    }
    for (const [ws, user] of this.users) {
      if (Date.now() - user.lastHeartbeat > this.userTimeoutMs) {
        const probed = this.livenessProbes.get(ws);
        if (probed === undefined) {
          this.livenessProbes.set(ws, Date.now());
          try { ws.ping(); } catch { /* socket 已坏，下轮关闭 */ }
          continue;
        }
        if (Date.now() - probed < 10_000) continue;
        logger.warn("user heartbeat timeout", { user_id: user.userID });
        this.livenessProbes.delete(ws);
        this.users.delete(ws);
        this.dropPendingFor(ws);
        ws.close();
      }
    }
  }

  // 用户连接消失时清掉它名下所有待应答请求
  private dropPendingFor(ws: WebSocket): void {
    for (const [reqID, p] of this.pendingRequests) {
      if (p.user.ws === ws) {
        clearTimeout(p.timer);
        this.pendingRequests.delete(reqID);
      }
    }
  }

  private registeredAgentOf(a: AgentConn): RegisteredAgent {
    return {
      id: a.id,
      owner_id: a.ownerID,
      name: a.name,
      status: a.status || proto.AGENT_STATUS_ONLINE,
      capabilities: a.capabilities,
      platform: a.platform,
      instance_id: this.bus?.instanceID ?? "",
      last_heartbeat: a.lastHeartbeat,
      brand_id: a.brandID ?? null,
      approval_status: a.approval ?? "approved",
    };
  }

  // 心跳/状态变化时刷新注册表 TTL 与内容（单机模式为 no-op）。
  // 刷新会重置 TTL：节流水位取 TTL 的 1/3，保证两次刷新之间条目不过期；
  // 状态/归属变化用 force 立即刷。
  refreshAgentRegistry(a: AgentConn, force = false): void {
    if (!this.bus || a.id === "") return;
    const now = Date.now();
    if (!force && now - (a.lastRegistryTouch ?? 0) < this.bus.registryTtlMs / 3) return;
    a.lastRegistryTouch = now;
    this.bus.refreshAgent(this.registeredAgentOf(a))
      .catch((e) => logger.error("registry refresh failed", { error: String(e) }));
  }

  registerAgent(a: AgentConn): void {
    a.lastHeartbeat = Date.now();
    this.agents.set(a.id, a);
    // pending（待审批）agent 不进注册表：其他实例不可见、不可接任务
    if (this.bus && a.approval !== "pending") {
      a.lastRegistryTouch = Date.now(); // register 已写入全量条目，首个心跳不必立刻刷新
      this.bus.registerAgent(this.registeredAgentOf(a))
        .catch((e) => logger.error("registry register failed", { error: String(e) }));
    }
    if (this.db) {
      a.lastDbTouch = Date.now();
      this.db.upsertAgent({
        id: a.id,
        owner_id: a.ownerID,
        name: a.name,
        platform: a.platform ? JSON.stringify(a.platform) : null,
        capabilities: JSON.stringify(a.capabilities),
        status: a.status || proto.AGENT_STATUS_ONLINE,
        last_ip: a.ip ?? null,
      }).catch((e) => logger.error("agent upsert failed", { error: String(e) }));
    }
    this.broadcastAgentList();
    this.broadcastAgentEvent("register", a.id, a.ownerID);
    logger.info("agent registered", { agent_id: a.id, owner_id: a.ownerID });
  }

  unregisterAgent(id: string): void {
    const ownerID = this.agents.get(id)?.ownerID ?? "";
    this.agents.delete(id);
    if (this.bus) {
      this.bus.unregisterAgent(id)
        .catch((e) => logger.error("registry unregister failed", { error: String(e) }));
    }
    if (this.db) {
      this.db.markAgentOffline(id)
        .catch((e) => logger.error("agent offline mark failed", { error: String(e) }));
    }
    this.broadcastAgentList();
    this.broadcastAgentEvent("offline", id, ownerID);
    logger.info("agent unregistered", { agent_id: id });
  }

  // ---- 品牌目录与 connector ----

  async reloadBrands(): Promise<void> {
    if (!this.db) return;
    const rows = await this.db.listBrands();
    this.brands = new Map(rows.map((b) => [b.id, b]));
  }

  // 品牌目录非空即进入治理模式：注册必须带品牌，client 主动注册需审批
  governanceOn(): boolean {
    return this.brands.size > 0;
  }

  registerConnector(a: AgentConn): void {
    a.lastHeartbeat = Date.now();
    if (a.connectorID) this.connectors.set(a.connectorID, a);
    logger.info("connector registered", { connector_id: a.connectorID, owner_id: a.ownerID });
  }

  // ws 不传时无条件删除（管理动作）；传了只删仍指向该连接的条目——
  // 防止旧连接迟到的 close 事件把新连接（重连恢复）的 connector 条目误删
  unregisterConnector(id: string, ws?: WebSocket): void {
    const cur = this.connectors.get(id);
    if (!cur || (ws !== undefined && cur.ws !== ws)) return;
    this.connectors.delete(id);
    logger.info("connector unregistered", { connector_id: id });
  }

  // 全量推送 connector 的目标 agent 集：本地投递 + 经总线广播到其他实例
  async pushConnectorSync(connectorID: string): Promise<void> {
    if (!this.db) return;
    const rows = await this.db.listConnectorAgents(connectorID);
    const agents: proto.ConnectorSyncAgent[] = rows.map((r) => {
      const brand = r.brand_id ? this.brands.get(r.brand_id) : undefined;
      let capabilities: proto.Capability[] = [];
      try { capabilities = brand?.capabilities ? JSON.parse(brand.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
      return {
        agent_id: r.id, brand_id: r.brand_id ?? "", name: r.name, capabilities,
        conn_type: brand?.conn_type || undefined,
        launch_cmd: brand?.launch_cmd ?? undefined,
        endpoint: brand?.endpoint ?? undefined,
      };
    });
    const msg = proto.newNotification(proto.METHOD_CONNECTOR_SYNC, { agents } satisfies proto.ConnectorSyncParams);
    this.deliverToLocalConnector(connectorID, msg);
    if (this.bus) {
      this.bus.publishConnectorSync(connectorID, msg)
        .catch((e) => logger.error("bus publish connector sync failed", { error: String(e) }));
    }
  }

  deliverToLocalConnector(connectorID: string, msg: proto.Message): void {
    const conn = this.connectors.get(connectorID);
    if (conn) this.trySend(conn.ws, msg);
  }

  // 审批结果落到在线连接（总线投递入口也走这里）：approved 补进注册表，rejected 踢线
  applyAgentApproval(agentID: string, status: string): void {
    const a = this.agents.get(agentID);
    if (a) {
      if (status === "approved") {
        a.approval = "approved";
        // pending 时未进注册表（键和集合都没有），这里走完整 register
        if (this.bus) {
          a.lastRegistryTouch = Date.now();
          this.bus.registerAgent(this.registeredAgentOf(a))
            .catch((e) => logger.error("registry register failed", { error: String(e) }));
        }
      } else if (status === "rejected") {
        a.ws.close(4001, "registration rejected");
      }
    }
    this.broadcastAgentList();
  }

  // 心跳/状态变化驱动 agents 表状态更新，60s 节流避免写放大（force 用于状态切换）
  touchAgentThrottled(a: AgentConn, force = false): void {
    if (!this.db || a.id === "") return;
    const now = Date.now();
    if (!force && now - (a.lastDbTouch ?? 0) < 60_000) return;
    a.lastDbTouch = now;
    this.db.touchAgent(a.id, a.status || proto.AGENT_STATUS_ONLINE)
      .catch((e) => logger.error("agent touch failed", { error: String(e) }));
  }

  registerUser(u: UserConn): void {
    u.lastHeartbeat = Date.now();
    this.users.set(u.ws, u);
    this.sendAgentList(u);
  }

  unregisterUser(ws: WebSocket): void {
    this.users.delete(ws);
    this.dropPendingFor(ws);
  }

  // 关闭本实例上匹配的连接：页面连接按 userID，agent 连接按 ownerID 或设备密钥 id。
  // 总线投递入口也走这里（不会再回传总线）。
  kickLocal(userID?: string, deviceKeyID?: string, reason = "kicked"): void {
    for (const u of this.users.values()) {
      if (userID !== undefined && u.userID === userID) u.ws.close(4001, reason);
    }
    for (const a of this.agents.values()) {
      if ((userID !== undefined && a.ownerID === userID)
        || (deviceKeyID !== undefined && a.deviceKeyID === deviceKeyID)) {
        a.ws.close(4001, reason);
      }
    }
  }

  // 禁用/改密后踢掉该用户的所有页面连接；多实例时经总线踢其他实例上的连接
  // （agent 连接也按属主一并踢掉，未踢到的下次心跳超时自然清理）
  kickUser(userID: string, reason = "account disabled or password changed"): void {
    this.kickLocal(userID, undefined, reason);
    if (this.bus) {
      this.bus.publishKick(userID, undefined, reason)
        .catch((e) => logger.error("bus publish kick failed", { error: String(e) }));
    }
  }

  // 吊销设备密钥后踢掉使用它的在线 agent（含其他实例上的连接）
  kickDeviceKey(deviceKeyID: string): void {
    const reason = "device key revoked";
    this.kickLocal(undefined, deviceKeyID, reason);
    if (this.bus) {
      this.bus.publishKick(undefined, deviceKeyID, reason)
        .catch((e) => logger.error("bus publish kick failed", { error: String(e) }));
    }
  }

  getAgent(id: string): AgentConn | undefined {
    return this.agents.get(id);
  }

  // 跨实例解析 agent：先查本地连接，再查注册表
  async resolveAgent(agentID: string): Promise<{ id: string; ownerID: string } | undefined> {
    const local = this.agents.get(agentID);
    if (local) return { id: agentID, ownerID: local.ownerID };
    if (this.bus) {
      try {
        const remote = await this.bus.getAgent(agentID);
        if (remote) return { id: agentID, ownerID: remote.owner_id };
      } catch (e) {
        logger.error("registry get failed", { error: String(e) });
      }
    }
    return undefined;
  }

  // 该用户名下所有 agent id（本地 + 注册表）
  async resolveOwnerAgentIDs(ownerID: string): Promise<string[]> {
    const ids = new Set<string>();
    for (const [id, a] of this.agents) {
      if (a.ownerID === ownerID) ids.add(id);
    }
    if (this.bus) {
      try {
        for (const a of await this.bus.listAgents()) {
          if (a.owner_id === ownerID) ids.add(a.id);
        }
      } catch { /* 注册表不可用时仅本地 */ }
    }
    return [...ids];
  }

  canManage(user: UserConn, agent: { ownerID: string }): boolean {
    return user.userID !== "" && (user.isAdmin || user.userID === agent.ownerID);
  }

  // admin 角色判定缓存（5s TTL）：管理后台一串调用不再每次查库。
  // setRole/disable 会调 invalidateAdminCache 主动失效，并踢线强制重连。
  private adminCache = new Map<string, { isAdmin: boolean; expiresAt: number }>();

  async isAdminUser(userID: string, db: Db): Promise<boolean> {
    const hit = this.adminCache.get(userID);
    if (hit && hit.expiresAt > Date.now()) return hit.isAdmin;
    const me = await db.getUserById(userID);
    const isAdmin = me?.role === "admin";
    if (this.adminCache.size > 1024) {
      const now = Date.now();
      for (const [k, v] of this.adminCache) {
        if (v.expiresAt <= now) this.adminCache.delete(k);
      }
    }
    this.adminCache.set(userID, { isAdmin, expiresAt: Date.now() + 5_000 });
    return isAdmin;
  }

  invalidateAdminCache(userID: string): void {
    this.adminCache.delete(userID);
  }

  // 品牌信息附加到 AgentInfo（logo/品牌名），供页面展示
  private withBrand(info: proto.AgentInfo, brandID: string | null | undefined): proto.AgentInfo {
    info.brand_id = brandID ?? null;
    const brand = brandID ? this.brands.get(brandID) : undefined;
    info.brand_name = brand?.name ?? null;
    info.logo_url = brand?.logo_url ?? null;
    return info;
  }

  async agentList(): Promise<proto.AgentInfo[]> {
    const byID = new Map<string, proto.AgentInfo>();
    if (this.bus) {
      try {
        for (const a of await this.bus.listAgents()) {
          byID.set(a.id, this.withBrand({
            id: a.id,
            owner_id: a.owner_id,
            name: a.name,
            status: a.status || proto.AGENT_STATUS_ONLINE,
            capabilities: a.capabilities,
            platform: a.platform,
            last_heartbeat: new Date(a.last_heartbeat).toISOString(),
            approval_status: a.approval_status ?? "approved",
          }, a.brand_id));
        }
      } catch (e) {
        logger.error("registry list failed", { error: String(e) });
      }
    }
    // 本地连接的信息最新，覆盖注册表中的同 id 条目
    for (const [id, a] of this.agents) {
      byID.set(id, this.withBrand({
        id,
        owner_id: a.ownerID,
        name: a.name,
        status: a.approval === "pending" ? "pending" : (a.status || proto.AGENT_STATUS_ONLINE),
        capabilities: a.capabilities,
        platform: a.platform,
        last_heartbeat: new Date(a.lastHeartbeat).toISOString(),
        approval_status: a.approval ?? "approved",
      }, a.brandID));
    }
    return [...byID.values()];
  }

  private filterAgentsForUser(agents: proto.AgentInfo[], user: UserConn): proto.AgentInfo[] {
    if (user.isAdmin && !user.ownOnly) return agents;
    return agents.filter((a) => a.owner_id === user.userID);
  }

  broadcastAgentList(): void {
    // 1s 合并窗口：register/unregister/状态抖动集中刷新一次，避免写放大
    if (this.agentListTimer) return;
    this.agentListTimer = setTimeout(() => {
      this.agentListTimer = undefined;
      void this.agentList().then(async (agents) => {
        // 昵称按 owner 私有：一次查出全表，普通用户帧在序列化前盖上本人昵称
        const byOwner = new Map<string, Map<string, string>>();
        if (this.db) {
          for (const n of await this.db.listAllNicknames().catch((e) => {
            logger.error("list nicknames failed", { error: String(e) });
            return [] as { owner_id: string; agent_id: string; nickname: string }[];
          })) {
            let m = byOwner.get(n.owner_id);
            if (!m) { m = new Map(); byOwner.set(n.owner_id, m); }
            m.set(n.agent_id, n.nickname);
          }
        }
        // 同一份名单序列化一次：全量 admin（非 ownOnly）共享一帧（不含昵称），其余按用户各算一帧
        const cache = new Map<string, string>();
        for (const user of this.users.values()) {
          const key = user.isAdmin && !user.ownOnly ? "" : user.userID;
          let raw = cache.get(key);
          if (raw === undefined) {
            let filtered = this.filterAgentsForUser(agents, user);
            if (key !== "") {
              const nicks = byOwner.get(key);
              filtered = filtered.map((a) => ({ ...a, nickname: nicks?.get(a.id) ?? null }));
            }
            raw = JSON.stringify(proto.newNotification(proto.METHOD_ADMIN_AGENT_LIST, { agents: filtered }));
            cache.set(key, raw);
          }
          if (user.ws.readyState === WebSocket.OPEN) user.ws.send(raw);
        }
      }).catch((e) => logger.error("broadcast agent list failed", { error: String(e) }));
    }, 1_000);
    this.agentListTimer.unref();
  }

  private sendAgentList(u: UserConn): void {
    void this.agentList().then(async (agents) => {
      let filtered = this.filterAgentsForUser(agents, u);
      // 非全量 admin 视图（普通用户 / ownOnly admin）带本人昵称
      if (this.db && !(u.isAdmin && !u.ownOnly)) {
        const nicks = await this.db.listNicknamesForOwner(u.userID).catch(() => new Map<string, string>());
        filtered = filtered.map((a) => ({ ...a, nickname: nicks.get(a.id) ?? null }));
      }
      const msg = proto.newNotification(proto.METHOD_ADMIN_AGENT_LIST, { agents: filtered });
      this.trySend(u.ws, msg);
    }).catch(() => {});
  }

  broadcastAgentEvent(event: string, agentID: string, ownerID: string): void {
    const msg = proto.newNotification(proto.METHOD_ADMIN_AGENT_EVENT, {
      event,
      agent_id: agentID,
      timestamp: proto.rfc3339Now(),
    } satisfies proto.AgentEventParams);
    this.forwardToUsers(ownerID, msg);
    // admin 需要看到全量事件（跨实例时其他实例的 admin 靠 agent.list 刷新兜底）；
    // 对话页连接（ownOnly）不推送他人的事件
    for (const u of this.users.values()) {
      if (u.isAdmin && !u.ownOnly && u.userID !== ownerID) this.trySend(u.ws, msg);
    }
  }

  forwardToAgent(agentID: string, msg: proto.Message): void {
    // connector 多 agent 托管：注入 agent_id 供 client 把消息分派到对应 shim
    if ((msg.method === proto.METHOD_AGENT_CHAT || msg.method === proto.METHOD_AGENT_CANCEL
        || msg.method === proto.METHOD_AGENT_RESPOND)
      && msg.params !== null && typeof msg.params === "object"
      && (msg.params as { agent_id?: string }).agent_id === undefined) {
      (msg.params as Record<string, unknown>).agent_id = agentID;
    }
    const agent = this.getAgent(agentID);
    if (agent) {
      this.trySend(agent.ws, msg);
      return;
    }
    if (this.bus) {
      const bus = this.bus;
      void bus.getAgent(agentID).then((remote) => {
        if (remote) return bus.sendToAgent(remote.instance_id, agentID, msg);
        logger.warn("agent not found", { agent_id: agentID });
      }).catch((e) => logger.error("bus forward failed", { error: String(e) }));
      return;
    }
    logger.warn("agent not found", { agent_id: agentID });
  }

  // ownerID 为 "" 表示广播给所有用户
  forwardToUsers(ownerID: string, msg: proto.Message): void {
    for (const user of this.users.values()) {
      if (ownerID !== "" && user.userID !== ownerID) continue;
      this.trySend(user.ws, msg);
    }
    if (this.bus) {
      this.bus.publishUserMessage(ownerID, msg)
        .catch((e) => logger.error("bus publish user msg failed", { error: String(e) }));
    }
  }

  // 总线投递入口：只发给本实例的用户，不再回传总线
  deliverToLocalUsers(ownerID: string, msg: proto.Message): void {
    for (const user of this.users.values()) {
      if (ownerID !== "" && user.userID !== ownerID) continue;
      this.trySend(user.ws, msg);
    }
  }

  forwardToPendingUser(id: string, msg: proto.Message): void {
    if (this.deliverToLocalPending(id, msg)) return;
    if (this.bus) {
      this.bus.publishPending(id, msg)
        .catch((e) => logger.error("bus publish pending failed", { error: String(e) }));
    }
  }

  deliverToLocalAgent(agentID: string, msg: proto.Message): void {
    const agent = this.agents.get(agentID);
    if (agent) this.trySend(agent.ws, msg);
  }

  deliverToLocalPending(id: string, msg: proto.Message): boolean {
    const p = this.pendingRequests.get(id);
    if (!p) return false;
    clearTimeout(p.timer);
    this.pendingRequests.delete(id);
    this.trySend(p.user.ws, msg);
    return true;
  }

  trackPendingRequest(id: string, user: UserConn, taskID?: string): void {
    const old = this.pendingRequests.get(id);
    if (old) clearTimeout(old.timer);
    // agent 不应答时兜底：回错误并删条目，防止 pendingRequests 无限增长
    const timer = setTimeout(() => {
      if (!this.pendingRequests.delete(id)) return;
      logger.warn("pending request timeout", { request_id: id });
      this.trySend(user.ws, proto.newErrorResponse(id, proto.ERR_INTERNAL_ERROR, "request timeout"));
    }, this.pendingTimeoutMs);
    timer.unref();
    this.pendingRequests.set(id, { user, timer, taskID });
  }

  // agent 以 JSON-RPC 错误响应拒绝 agent.chat 时（1:1 路径带请求 id）：
  // 清理已 trackTask 的条目，否则任务挂到超时。须在 forwardToPendingUser 删除条目前调用。
  cleanupRejectedTask(requestID: string, reason: string): void {
    const p = this.pendingRequests.get(requestID);
    if (!p || !p.taskID) return;
    const ts = this.tasks.get(p.taskID);
    if (ts) this.notifySubtaskResult(p.taskID, ts, reason);
    this.observeTaskEnd(p.taskID, "failed");
    this.untrackTask(p.taskID);
  }

  trackTask(taskID: string, agentID: string, ownerID: string, sessionID = "", extra?: Partial<TaskState>): void {
    const timer = setTimeout(() => this.taskTimeoutCallback(taskID), this.taskTimeoutMs);
    timer.unref();
    this.tasks.set(taskID, { agentID, ownerID, sessionID, timer, createdAt: Date.now(), depth: 0, ...extra });
    this.metrics.inc("ywm_tasks_created_total");
  }

  untrackTask(taskID: string): void {
    const ts = this.tasks.get(taskID);
    if (ts) {
      clearTimeout(ts.timer);
      this.tasks.delete(taskID);
    }
  }

  // ---- 消息持久化（db 未配置时静默跳过，保持纯转发模式可运行） ----

  // agent_id 兼容群聊路径：群会话传 "group:<gid>"（会话与消息的归因标识）
  persistUserMessage(params: proto.TaskCreateParams, sessionID: string, ownerID: string): void {
    if (!this.db) return;
    const db = this.db;
    const agentID = params.agent_id ?? "";
    const now = Date.now();
    const run = async () => {
      const existing = await db.getSession(ownerID, sessionID);
      if (!existing) {
        const title = params.content.trim().replace(/\s+/g, " ").slice(0, 20) || "新会话";
        await db.createSession({ id: sessionID, owner_id: ownerID, agent_id: agentID, title });
      }
      await db.appendMessage({
        id: crypto.randomUUID(),
        session_id: sessionID,
        owner_id: ownerID,
        agent_id: agentID,
        role: "user",
        content: JSON.stringify({
          text: params.content,
          attachments: (params.metadata?.attachments as unknown) ?? undefined,
        }),
        task_id: params.task_id,
        created_at: now,
      });
      this.metrics.inc("ywm_messages_persisted_total");
      await db.touchSession(sessionID, now);
    };
    run().catch((e) => logger.error("persist user message failed", { error: String(e) }));
  }

  bufferProgressChunk(taskID: string, ownerID: string, agentID: string, sessionID: string, chunk: proto.LocalAgentChunk): void {
    if (!this.db) return;
    let buf = this.taskBuffers.get(taskID);
    if (!buf) {
      buf = { ownerID, agentID, sessionID, chunks: [], bytes: 0, truncated: false };
      this.taskBuffers.set(taskID, buf);
    }
    const size = JSON.stringify(chunk).length;
    if (buf.bytes + size > MAX_TASK_BUFFER_BYTES) {
      if (!buf.truncated) {
        buf.truncated = true;
        logger.warn("task buffer truncated", { task_id: taskID, bytes: buf.bytes });
      }
      return;
    }
    buf.bytes += size;
    buf.chunks.push(chunk);
  }

  // 交互应答持久化：把缓冲中的待决交互 chunk 标记为已答（answer 原样随任务落库），
  // 前端从历史加载时据此渲染"已回复"而非重新弹待确认框
  markRespondedChunk(taskID: string, confirmID: string, promptID: string, blockID: string, response: unknown): void {
    const buf = this.taskBuffers.get(taskID);
    if (!buf) return;
    for (const c of buf.chunks as Array<BufferedInteractionChunk>) {
      if (c.answered || !isInteractionChunk(c)) continue;
      if (matchesInteractionChunk(c, confirmID, promptID, blockID)) {
        c.answered = true;
        c.answer = response ?? null;
      }
    }
  }

  // 交互框撤销（confirm_cancelled / 任务终结）：待决交互 chunk 标记 cancelled
  markCancelledChunks(taskID: string, confirmID: string, reason: string): void {
    const buf = this.taskBuffers.get(taskID);
    if (!buf) return;
    for (const c of buf.chunks as Array<BufferedInteractionChunk>) {
      if (c.answered || c.cancelled || !isInteractionChunk(c)) continue;
      if (confirmID === "" || c.confirm_id === confirmID) {
        c.cancelled = true;
        c.reason = reason;
      }
    }
  }

  flushTaskBuffer(taskID: string, errorText?: string): void {
    const buf = this.taskBuffers.get(taskID);
    if (!buf || !this.db) {
      this.taskBuffers.delete(taskID);
      return;
    }
    this.taskBuffers.delete(taskID);
    // 任务终结时仍未应答的交互框一律标记撤销（与前端实时兜底"已撤销：任务结束"对齐）
    for (const c of buf.chunks as Array<BufferedInteractionChunk>) {
      if (!c.answered && !c.cancelled && isInteractionChunk(c)) {
        c.cancelled = true;
        c.reason = errorText || "task ended";
      }
    }
    if (buf.truncated) buf.chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent("（输出过长，中间内容已截断）") });
    if (errorText) buf.chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(errorText) });
    if (buf.chunks.length === 0) return;
    const db = this.db;
    const now = Date.now();
    db.appendMessage({
      id: crypto.randomUUID(),
      session_id: buf.sessionID,
      owner_id: buf.ownerID,
      agent_id: buf.agentID,
      role: "assistant",
      content: JSON.stringify({ chunks: buf.chunks }),
      task_id: taskID,
      created_at: now,
    }).then(() => {
      this.metrics.inc("ywm_messages_persisted_total");
      return db.touchSession(buf.sessionID, now);
    })
      .catch((e) => logger.error("persist assistant message failed", { error: String(e) }));
  }

  // 管理者编排：子任务终结（done/error/timeout）时把结果回投给管理者 agent
  notifySubtaskResult(taskID: string, ts: TaskState, errorText?: string): void {
    if (!ts.parentTaskID || !ts.invokerAgentID) return;
    const buf = this.taskBuffers.get(taskID);
    const chunks = [...(buf?.chunks ?? [])];
    if (buf?.truncated) chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent("（输出过长，中间内容已截断）") });
    if (errorText) chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(errorText) });
    this.forwardToAgent(ts.invokerAgentID, proto.newNotification(proto.METHOD_AGENT_TASK_RESULT, {
      agent_id: ts.invokerAgentID, // connector 多实例托管时 client 按此路由到管理者实例
      task_id: taskID,
      parent_task_id: ts.parentTaskID,
      group_id: ts.groupID ?? "",
      target_agent_id: ts.agentID,
      status: errorText ? "failed" : "completed",
      chunks,
      error: errorText,
    } satisfies proto.AgentTaskResultParams));
  }

  // C6 父子任务取消：父任务取消/超时时，按 parentTaskID 级联取消未完成子任务。
  // tasks 只存未完成任务（done 即 untrack），无需再筛状态；depth 硬限 1，
  // 子任务不能再派生孙任务，单层匹配即可。任务清理仍由各子任务 done 进度驱动。
  cascadeCancelSubtasks(parentTaskID: string, allow?: (ts: TaskState) => boolean): void {
    for (const [tid, ts] of this.tasks) {
      if (ts.parentTaskID !== parentTaskID) continue;
      if (allow && !allow(ts)) continue;
      logger.info("cascade cancel subtask", { task_id: tid, parent_task_id: parentTaskID });
      this.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
        task_id: tid,
        session_id: ts.sessionID || undefined,
      } satisfies proto.AgentCancelParams));
    }
  }

  private taskTimeoutCallback(taskID: string): void {
    const ts = this.tasks.get(taskID);
    if (!ts) return;
    this.observeTaskEnd(taskID, "timeout");
    this.tasks.delete(taskID);
    logger.warn("task timeout", { task_id: taskID, agent_id: ts.agentID });
    this.notifySubtaskResult(taskID, ts, "任务超时");
    this.flushTaskBuffer(taskID, "任务超时");
    // 通知 agent 中止任务，避免网关侧超时后 agent 还在空跑
    this.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
      task_id: taskID,
      session_id: ts.sessionID || undefined,
    } satisfies proto.AgentCancelParams));
    this.cascadeCancelSubtasks(taskID);
    const notif = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, {
      task_id: taskID,
      agent_id: ts.agentID,
      done: true,
      error: "timeout",
    } satisfies proto.AdminProgressParams);
    this.forwardToUsers(ts.ownerID, notif);
  }

  trySend(ws: WebSocket, msg: proto.Message): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(msg));
  }
}

function sendError(ws: WebSocket, id: string | undefined, code: number, message: string, data?: unknown): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(proto.newErrorResponse(id ?? "", code, message, data)));
}

function sendMsg(ws: WebSocket, msg: proto.Message): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(msg));
}

// db 未配置时返回错误；否则执行异步存储操作并把异常转为 JSON-RPC 错误。
function withDb(hub: Hub, user: UserConn, msg: proto.Message, fn: (db: Db) => Promise<void>): void {
  if (!hub.db) {
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  fn(hub.db).catch((e) => {
    logger.error("storage op failed", { method: msg.method, error: String(e) });
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage error");
  });
}

async function handleSessionList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionListParams>(msg);
  const sessions = await db.listSessions(user.userID, params.agent_id || undefined);
  const lastMsgs = await db.listLastMessages(user.userID);
  const previews = new Map(lastMsgs.map((m) => [m.session_id, storedPreview(m.role, m.content)]));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    sessions: sessions.map((s) => ({
      id: s.id,
      agent_id: s.agent_id,
      title: s.title,
      workdir: s.workdir ?? null,
      created_at: s.created_at,
      updated_at: s.updated_at,
      message_count: Number(s.message_count ?? 0),
      preview: previews.get(s.id) || "",
    })),
  } satisfies proto.SessionListResult));
}

// 从落库的消息 content JSON 里提取一句可展示的摘要（≤80 字）
function storedPreview(role: string, content: string): string {
  try {
    const c = JSON.parse(content) as { text?: string; chunks?: { type?: string; text?: string; content?: unknown }[]; attachments?: { name?: string }[] };
    let text = "";
    if (role === "user") {
      text = c.text || "";
      if (!text && c.attachments?.length) text = "[附件] " + (c.attachments[0].name || "");
    } else {
      const t = (c.chunks || []).find((ch) => ch.type === "text");
      if (t) text = typeof t.text === "string" ? t.text : "";
      if (!text && c.chunks?.length) text = "[" + (c.chunks[c.chunks.length - 1].type || "chunk") + "]";
    }
    text = text.replace(/[#>*`\-]/g, " ").replace(/\s+/g, " ").trim();
    return text.length > 80 ? text.slice(0, 80) + "…" : text;
  } catch {
    return "";
  }
}

async function handleSessionCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionCreateParams>(msg);
  if (!params.agent_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  const existing = params.id ? await db.getSession(user.userID, params.id) : undefined;
  if (existing) {
    sendMsg(user.ws, proto.newResponse(msg.id ?? "", sessionInfoOf(existing)));
    return;
  }
  // 目标归属校验：agent 型需本人可管理（查库，离线 agent 亦可建）；
  // 群组型需群主本人。否则任何用户都能往别人的 agent 下挂脏会话行。
  if (params.agent_id.startsWith("group:")) {
    if (!user.isAdmin && !(await db.getGroup(user.userID, params.agent_id.slice("group:".length)))) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not your group");
      return;
    }
  } else {
    const row = await db.getAgentRow(params.agent_id);
    if (!row || !hub.canManage(user, { ownerID: row.owner_id })) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to create session for this agent");
      return;
    }
  }
  const workdir = params.workdir?.trim() || "";
  if (workdir.length > 512) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "workdir too long (max 512)");
    return;
  }
  const s = await db.createSession({
    id: params.id || crypto.randomUUID(),
    owner_id: user.userID,
    agent_id: params.agent_id,
    title: params.title?.trim() || "新会话",
    workdir: workdir || null,
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", sessionInfoOf(s)));
}

function sessionInfoOf(s: { id: string; agent_id: string; title: string; workdir?: string | null; created_at: number; updated_at: number; message_count?: number }): proto.SessionInfo {
  return {
    id: s.id,
    agent_id: s.agent_id,
    title: s.title,
    workdir: s.workdir ?? null,
    created_at: s.created_at,
    updated_at: s.updated_at,
    message_count: Number(s.message_count ?? 0),
  };
}

async function handleSessionSetWorkdir(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionSetWorkdirParams>(msg);
  const workdir = params.workdir.trim();
  if (workdir.length > 512) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "workdir too long (max 512)");
    return;
  }
  const ok = await db.setSessionWorkdir(user.userID, params.id, workdir === "" ? null : workdir);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", workdir: workdir === "" ? null : workdir }));
}

async function handleSessionRename(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionRenameParams>(msg);
  const ok = await db.renameSession(user.userID, params.id, params.title);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 删除会话并级联清理消息里引用的附件文件（本地盘 / S3 通用）
async function deleteSessionWithAttachments(hub: Hub, ownerID: string, sessionID: string): Promise<boolean> {
  const db = hub.db;
  if (!db) return false;
  const keys: string[] = [];
  if (hub.attachments) {
    let before: number | undefined;
    for (;;) {
      const rows = await db.listMessages(ownerID, sessionID, 500, before);
      if (rows.length === 0) break;
      for (const m of rows) {
        try {
          const c = JSON.parse(m.content) as { attachments?: { url?: string }[] };
          for (const a of c.attachments ?? []) {
            const k = a.url ? hub.attachments.keyFromUrl(a.url) : undefined;
            if (k) keys.push(k);
          }
        } catch { /* 非 JSON 内容跳过 */ }
      }
      if (rows.length < 500) break;
      before = rows[0].created_at;
    }
  }
  const ok = await db.deleteSession(ownerID, sessionID);
  if (ok && hub.attachments) {
    for (const k of keys) {
      await hub.attachments.delete(k)
        .catch((e) => logger.warn("attachment delete failed", { key: k, error: String(e) }));
    }
  }
  return ok;
}

async function handleSessionDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionDeleteParams>(msg);
  const ok = await deleteSessionWithAttachments(hub, user.userID, params.id);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleMessageList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.MessageListParams>(msg);
  const session = await db.getSession(user.userID, params.session_id);
  if (!session) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const [rows, total] = await Promise.all([
    db.listMessages(user.userID, params.session_id, limit, params.before),
    db.countMessages(user.userID, params.session_id),
  ]);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    messages: rows.map((m) => ({
      id: m.id,
      session_id: m.session_id,
      agent_id: m.agent_id,
      role: m.role,
      content: JSON.parse(m.content) as unknown,
      task_id: m.task_id,
      created_at: m.created_at,
    })),
    total,
  } satisfies proto.MessageListResult));
}

// ---- 群组（多 agent 会话）----

function groupInfoOf(g: { id: string; name: string; manager_agent_id: string | null; created_at: number }, agentIDs: string[]): proto.GroupInfo {
  return { id: g.id, name: g.name, manager_agent_id: g.manager_agent_id, agent_ids: agentIDs, created_at: g.created_at };
}

// 校验 agent 归属：admin 可用他人 agent，普通用户仅自己的
async function requireOwnedAgent(db: Db, user: UserConn, agentID: string): Promise<boolean> {
  const row = await db.getAgentRow(agentID);
  return !!row && (user.isAdmin || row.owner_id === user.userID);
}

// 备注名仅属主可设（昵称按 owner 私有，只影响自己的显示）
async function handleAgentSetNickname(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentSetNicknameParams>(msg);
  if (!params.agent_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  if (!(await requireOwnedAgent(db, user, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not available");
    return;
  }
  const nickname = params.nickname?.trim().slice(0, 256) ?? "";
  await db.setNickname(user.userID, params.agent_id, nickname === "" ? null : nickname);
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    status: "ok",
    nickname: nickname === "" ? null : nickname,
  } satisfies proto.AgentSetNicknameResult));
}

async function handleGroupCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {  const params = proto.decodeParams<proto.GroupCreateParams>(msg);
  const agentIDs = [...new Set(params.agent_ids ?? [])];
  if (!params.name?.trim()) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (agentIDs.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_ids required");
    return;
  }
  for (const id of agentIDs) {
    if (!(await requireOwnedAgent(db, user, id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `agent not available: ${id}`);
      return;
    }
  }
  if (params.manager_agent_id && !agentIDs.includes(params.manager_agent_id)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "manager_agent_id must be a group member");
    return;
  }
  const groupID = crypto.randomUUID();
  const g = await db.createGroup({
    id: groupID,
    owner_id: user.userID,
    name: params.name.trim().slice(0, 128),
    manager_agent_id: params.manager_agent_id ?? null,
  });
  for (const id of agentIDs) await db.addGroupMember(groupID, id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { group_id: groupID } satisfies proto.GroupCreateResult));
}

async function handleGroupList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const groups = await db.listGroups(user.userID);
  const infos: proto.GroupInfo[] = [];
  for (const g of groups) {
    infos.push(groupInfoOf(g, await db.listGroupMembers(g.id)));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { groups: infos } satisfies proto.GroupListResult));
}

async function handleGroupDetail(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupDetailParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { group: groupInfoOf(g, await db.listGroupMembers(g.id)) } satisfies proto.GroupDetailResult));
}

async function handleGroupAdd(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupAddParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (!(await requireOwnedAgent(db, user, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not available");
    return;
  }
  await db.addGroupMember(params.group_id, params.agent_id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleGroupRemove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupRemoveParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (!(await db.removeGroupMember(params.group_id, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not in group");
    return;
  }
  if (g.manager_agent_id === params.agent_id) await db.setGroupManager(user.userID, params.group_id, null);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleGroupRename(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupRenameParams>(msg);
  const name = (params.name || "").trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (!(await db.getGroup(user.userID, params.group_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  await db.renameGroup(user.userID, params.group_id, name.slice(0, 128));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleGroupSetManager(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupSetManagerParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (params.manager_agent_id) {
    const members = await db.listGroupMembers(g.id);
    if (!members.includes(params.manager_agent_id)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "manager_agent_id must be a group member");
      return;
    }
  }
  await db.setGroupManager(user.userID, params.group_id, params.manager_agent_id ?? null);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleGroupDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupDeleteParams>(msg);
  if (!(await db.deleteGroup(user.userID, params.group_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 用户管理 ----

function userInfoOf(u: DbUser): proto.UserInfo {
  return { id: u.id, name: u.name, role: u.role, disabled: u.disabled === 1, created_at: u.created_at, last_login_at: u.last_login_at };
}

async function requireAdmin(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<boolean> {
  if (!(await hub.isAdminUser(user.userID, db))) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "admin only");
    return false;
  }
  return true;
}

async function handleUserList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserListParams>(msg);
  // 无分页参数时保持旧行为（全量），管理后台走分页路径
  if (params.query === undefined && params.limit === undefined && params.offset === undefined) {
    const users = await db.listUsers();
    sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
      users: users.map(userInfoOf),
      total: users.length,
    } satisfies proto.UserListResult));
    return;
  }
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const offset = Math.max(params.offset ?? 0, 0);
  const { users, total } = await db.listUsersPaged({ query: params.query || undefined, limit, offset });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    users: users.map(userInfoOf),
    total,
  } satisfies proto.UserListResult));
}

async function handleUserSetRole(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserSetRoleParams>(msg);
  if (params.role !== "admin" && params.role !== "user") {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "role must be admin or user");
    return;
  }
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot change your own role");
    return;
  }
  if (!(await db.setUserRole(params.id, params.role))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.invalidateAdminCache(params.id);
  hub.kickUser(params.id); // isAdmin 缓存在连接上，强制重连刷新
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleUserCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserCreateParams>(msg);
  if (!params.name || !params.password) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name and password required");
    return;
  }
  if (await db.getUserByName(params.name)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name already taken");
    return;
  }
  let id: string = crypto.randomUUID();
  const manualID = (params.id || "").trim();
  if (manualID) {
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(manualID)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id must be 1-64 chars of A-Za-z0-9._-");
      return;
    }
    if (await db.getUserById(manualID)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id already taken");
      return;
    }
    id = manualID;
  }
  const u: DbUser = {
    id,
    name: params.name,
    password_hash: hashPassword(params.password),
    role: params.role === "admin" ? "admin" : "user",
    disabled: 0,
    created_at: Date.now(),
    last_login_at: null,
    employee_id: null,
    display_name: null,
  };
  await db.createUser(u);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", userInfoOf(u)));
}

async function handleUserDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserDeleteParams>(msg);
  if (!params.id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id required");
    return;
  }
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot delete yourself");
    return;
  }
  if (!(await db.getUserById(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  const { agents } = await db.listAgentsPaged({ ownerID: params.id, limit: 100_000, offset: 0 });
  // purge 先于 agents 行删除：群成员清理的子查询依赖 agents 表仍含这些行
  await db.purgeUserOwnedData(params.id);
  for (const row of agents) {
    await doRemoveAgent(hub, db, row);
  }
  await db.deleteUser(params.id);
  hub.invalidateAdminCache(params.id);
  hub.kickUser(params.id);
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleUserDisable(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserDisableParams>(msg);
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot disable yourself");
    return;
  }
  if (!(await db.setUserDisabled(params.id, params.disabled))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.invalidateAdminCache(params.id);
  if (params.disabled) hub.kickUser(params.id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleUserResetPassword(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserResetPasswordParams>(msg);
  if (!params.id || !params.password) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id and password required");
    return;
  }
  if (!(await db.setUserPassword(params.id, hashPassword(params.password)))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.kickUser(params.id); // 强制重新登录
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleUserChangePassword(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.UserChangePasswordParams>(msg);
  const me = await db.getUserById(user.userID);
  if (!me || !verifyPassword(params.old_password ?? "", me.password_hash)) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "old password incorrect");
    return;
  }
  if (!params.new_password || params.new_password.length < 6) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "new password too short (>=6)");
    return;
  }
  await db.setUserPassword(me.id, hashPassword(params.new_password));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 管理后台：agent 管理与概览 ----

async function handleAgentList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  // admin 可查全量（可按 owner_id 过滤）；普通用户强制限定为本人名下
  const isAdmin = await hub.isAdminUser(user.userID, db);
  const params = proto.decodeParams<proto.AdminAgentListParams>(msg);
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const offset = Math.max(params.offset ?? 0, 0);
  const ownerFilter = isAdmin ? (params.owner_id || undefined) : user.userID;
  const { agents, total } = await db.listAgentsPaged({
    ownerID: ownerFilter,
    status: params.status || undefined,
    query: params.query || undefined,
    limit,
    offset,
  });
  const nicks = await db.listNicknamesForOwner(ownerFilter ?? user.userID);
  // 实时状态优先：内存/注册表覆盖 DB 行的展示状态，DB 提供离线/历史记录
  const liveByID = new Map<string, proto.AgentInfo>();
  for (const a of await hub.agentList()) liveByID.set(a.id, a);
  const rows: proto.AdminAgentInfo[] = agents.map((row) => {
    const live = liveByID.get(row.id);
    let caps: proto.Capability[] = [];
    let plat: proto.PlatformInfo | undefined;
    try { caps = row.capabilities ? JSON.parse(row.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
    try { plat = row.platform ? JSON.parse(row.platform) as proto.PlatformInfo : undefined; } catch { /* 忽略坏数据 */ }
    const brand = row.brand_id ? hub.brands.get(row.brand_id) : undefined;
    return {
      id: row.id,
      owner_id: live?.owner_id ?? row.owner_id,
      name: live?.name ?? row.name,
      nickname: nicks.get(row.id) ?? null,
      status: live?.status ?? (row.approval_status === "pending" ? "pending" : row.status),
      capabilities: live?.capabilities ?? caps,
      platform: live?.platform ?? plat,
      last_heartbeat: live?.last_heartbeat ?? new Date(row.last_seen).toISOString(),
      first_seen: row.first_seen,
      last_seen: row.last_seen,
      online: live !== undefined,
      last_ip: row.last_ip ?? null,
      brand_id: row.brand_id,
      brand_name: brand?.name ?? null,
      logo_url: brand?.logo_url ?? null,
      approval_status: row.approval_status,
      connector_id: row.connector_id,
    };
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { agents: rows, total } satisfies proto.AdminAgentListResult));
}

async function handleAgentDisconnect(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentDisconnectParams>(msg);
  const local = hub.getAgent(params.agent_id);
  if (!local) {
    // 一期限制：只能断连落在本实例上的 agent（跨实例需要 bus 指令通道）
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not connected to this instance");
    return;
  }
  if (local.connectorID) {
    // connector 托管的 agent 与兄弟 agent 共享连接，断连会误伤；移除请用 agent.remove
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "connector-managed agent: use agent.remove");
    return;
  }
  local.ws.close(4001, "disconnected by admin");
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleAgentReassign(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentReassignParams>(msg);
  if (!params.agent_id || !params.owner_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id and owner_id required");
    return;
  }
  if (!(await db.getUserById(params.owner_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "new owner not found");
    return;
  }
  if (!(await db.reassignAgent(params.agent_id, params.owner_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  // 在线连接同步改归属。注意：JWT 重连会按 token 夺回归属（upsertAgent 以凭证为准），
  // 所以 reassign 只对设备密钥接入的 agent 是持久的。
  const local = hub.getAgent(params.agent_id);
  if (local) {
    local.ownerID = params.owner_id;
    hub.refreshAgentRegistry(local, true);
  }
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleAdminOverview(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const [users, agentsTotal, liveAgents] = await Promise.all([
    db.listUsers(),
    db.countAgents(),
    hub.agentList(),
  ]);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    users_total: users.length,
    agents_total: agentsTotal,
    agents_online: liveAgents.length,
    users_connected: hub.users.size,
    tasks_active: hub.tasks.size,
  } satisfies proto.OverviewResult));
}

// ---- 设备密钥 ----

// 明文形如 amk_<base64url(24B)>，只在创建时返回一次；库中只存 sha256
function generateDeviceKey(): { plaintext: string; hash: string } {
  const plaintext = "amk_" + crypto.randomBytes(24).toString("base64url");
  return { plaintext, hash: hashDeviceKey(plaintext) };
}

function hashDeviceKey(plaintext: string): string {
  return crypto.createHash("sha256").update(plaintext).digest("hex");
}

function deviceKeyInfoOf(k: { id: string; owner_id: string; name: string; created_at: number; last_used_at: number | null; disabled: number }): proto.DeviceKeyInfo {
  return {
    id: k.id,
    owner_id: k.owner_id,
    name: k.name,
    created_at: k.created_at,
    last_used_at: k.last_used_at,
    disabled: k.disabled === 1,
  };
}

async function handleDeviceKeyCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyCreateParams>(msg);
  if (!params.name || !params.name.trim()) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.getUserById(params.owner_id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "owner not found");
      return;
    }
    ownerID = params.owner_id;
  }
  if (!hub.deviceKeyLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many keys created, please slow down");
    return;
  }
  const { plaintext, hash } = generateDeviceKey();
  const id = crypto.randomUUID();
  await db.createDeviceKey({ id, owner_id: ownerID, name: params.name.trim(), key_hash: hash });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { id, key: plaintext } satisfies proto.DeviceKeyCreateResult));
}

async function handleDeviceKeyList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyListParams>(msg);
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    ownerID = params.owner_id;
  }
  const keys = await db.listDeviceKeys(ownerID);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    keys: keys.map(deviceKeyInfoOf),
  } satisfies proto.DeviceKeyListResult));
}

async function handleDeviceKeyRevoke(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyRevokeParams>(msg);
  const keys = await db.listDeviceKeys(user.userID);
  let target = keys.find((k) => k.id === params.id);
  if (!target) {
    // 非本人需 admin 才能吊销（全表按 id 直接置禁用）
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.setDeviceKeyDisabled(params.id, true))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "key not found");
      return;
    }
  } else {
    await db.setDeviceKeyDisabled(target.id, true);
  }
  // 踢掉使用该密钥的在线 agent 连接（含其他实例）
  hub.kickDeviceKey(params.id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 配对接入：配对码 CRUD + 待接入审批 ----

function pairingCodeInfoOf(c: DbPairingCode): proto.PairingCodeInfo {
  return {
    id: c.id,
    owner_id: c.owner_id,
    expires_at: c.expires_at,
    used_at: c.used_at,
    created_at: c.created_at,
  };
}

async function handlePairingCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.PairingCodeCreateParams>(msg);
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.getUserById(params.owner_id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "owner not found");
      return;
    }
    ownerID = params.owner_id;
  }
  if (!hub.deviceKeyLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many codes created, please slow down");
    return;
  }
  const ttlMs = Math.min(Math.max(params.ttl_seconds ?? 86_400, 60), 7 * 86_400) * 1000;
  const { plaintext, hash } = generateDeviceKey();
  const id = crypto.randomUUID();
  const expiresAt = Date.now() + ttlMs;
  await db.createPairingCode({ id, owner_id: ownerID, code_hash: hash, expires_at: expiresAt });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    id, code: plaintext, owner_id: ownerID, expires_at: expiresAt,
  } satisfies proto.PairingCodeCreateResult));
}

async function handlePairingList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const codes = await db.listPairingCodes(user.userID);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    codes: codes.map(pairingCodeInfoOf),
  } satisfies proto.PairingCodeListResult));
}

async function handlePairingDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.PairingCodeDeleteParams>(msg);
  const codes = await db.listPairingCodes(user.userID);
  if (!codes.some((c) => c.id === params.id)) {
    // 非本人需 admin 才能作废
    if (!(await requireAdmin(hub, user, msg, db))) return;
  }
  if (!(await db.deletePairingCode(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "code not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

function pendingConnectorInfoOf(p: PendingPair): proto.PendingConnectorInfo {
  return {
    connector_id: p.connectorID,
    owner_id: p.ownerID,
    code_id: p.codeID,
    platform: p.platform,
    version: p.version,
    ip: p.ip,
    paired_at: p.pairedAt,
  };
}

async function handleConnectorPendingList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    connectors: [...hub.pendingPairs.values()].map(pendingConnectorInfoOf),
  } satisfies proto.ConnectorPendingListResult));
}

async function handleConnectorApprove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.ConnectorApproveParams>(msg);
  const p = hub.pendingPairs.get(params.connector_id);
  if (!p) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "pending connector not found");
    return;
  }
  // 签发设备密钥：owner = 配对码归属用户，明文只随 connector.credential 推一次
  const { plaintext, hash } = generateDeviceKey();
  await db.createDeviceKey({
    id: crypto.randomUUID(), owner_id: p.ownerID,
    name: `connector:${p.connectorID}`, key_hash: hash,
  });
  await db.markPairingCodeUsed(p.codeID);
  hub.pendingPairs.delete(p.connectorID);
  sendMsg(p.conn.ws, proto.newNotification(proto.METHOD_CONNECTOR_CREDENTIAL, {
    connector_id: p.connectorID, key: plaintext,
  } satisfies proto.ConnectorCredentialParams));
  logger.info("connector approved", { connector_id: p.connectorID, owner_id: p.ownerID, by: user.userID });
  // 凭证已投递，配对连接使命完成；client 落盘后用 key 重连走 connector.hello
  setTimeout(() => p.conn.ws.close(1000, "credential delivered"), 500).unref();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleConnectorReject(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.ConnectorApproveParams>(msg);
  const p = hub.pendingPairs.get(params.connector_id);
  if (!p) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "pending connector not found");
    return;
  }
  hub.pendingPairs.delete(p.connectorID);
  // 配对码不消耗，持码方可换 connector_id 重试或让管理员重新审批
  logger.info("connector rejected", { connector_id: p.connectorID, by: user.userID });
  p.conn.ws.close(4001, "pairing rejected");
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// client 凭配对码接入（?pair=1 无凭证连接）：校验码 → 挂起等审批
async function handleConnectorPair(hub: Hub, agent: AgentConn, msg: proto.Message): Promise<void> {
  if (!hub.db) {
    sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
    return;
  }
  const params = proto.decodeParams<proto.ConnectorPairParams>(msg);
  const connectorID = params.connector_id?.trim();
  if (!connectorID || !params.code?.trim()) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "code and connector_id required");
    return;
  }
  const code = await hub.db.getPairingCodeByHash(hashDeviceKey(params.code.trim()));
  if (!code || code.used_at !== null || code.expires_at <= Date.now()) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "invalid or expired pairing code");
    agent.ws.close(4001, "invalid pairing code");
    return;
  }
  // 同 connector_id 重复 pair：踢掉旧的挂起连接
  const old = hub.pendingPairs.get(connectorID);
  if (old && old.conn.ws !== agent.ws) old.conn.ws.close(4001, "superseded by new pairing");
  hub.pendingPairs.set(connectorID, {
    connectorID,
    ownerID: code.owner_id,
    codeID: code.id,
    conn: agent,
    platform: params.platform,
    version: params.version,
    ip: agent.ip,
    pairedAt: Date.now(),
  });
  logger.info("connector pairing pending", { connector_id: connectorID, owner_id: code.owner_id });
  sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "pending" } satisfies proto.ConnectorPairResult));
}

// ---- 品牌目录管理 ----

function brandInfoOf(b: DbAgentBrand): proto.BrandInfo {
  let caps: proto.Capability[] = [];
  try { caps = b.capabilities ? JSON.parse(b.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
  return {
    id: b.id,
    name: b.name,
    description: b.description,
    logo_url: b.logo_url,
    capabilities: caps,
    conn_type: b.conn_type || "stdio",
    launch_cmd: b.launch_cmd ?? null,
    endpoint: b.endpoint ?? null,
    disabled: b.disabled === 1,
    created_at: b.created_at,
    updated_at: b.updated_at,
  };
}

async function handleBrandList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  // 只读目录：任何登录用户可读（发起配对/了解可接入的品牌）；写操作仍仅 admin
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    brands: (await db.listBrands()).map(brandInfoOf),
  } satisfies proto.BrandListResult));
}

async function handleBrandCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandCreateParams>(msg);
  const name = params.name?.trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (await db.getBrandByName(name)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand name already exists");
    return;
  }
  const connType = params.conn_type ?? "stdio";
  if (!["stdio", "http", "ws"].includes(connType)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "conn_type must be stdio|http|ws");
    return;
  }
  const b: DbAgentBrand = {
    id: crypto.randomUUID(),
    name,
    description: params.description ?? "",
    logo_url: params.logo_url ?? null,
    capabilities: JSON.stringify(params.capabilities ?? []),
    conn_type: connType,
    launch_cmd: params.launch_cmd ?? null,
    endpoint: params.endpoint ?? null,
    disabled: 0,
    created_at: Date.now(),
    updated_at: Date.now(),
  };
  await db.createBrand(b);
  await hub.reloadBrands();
  hub.broadcastAgentList(); // 首个品牌会开启治理模式，列表刷新带品牌信息
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", brandInfoOf(b)));
}

async function handleBrandUpdate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandUpdateParams>(msg);
  const existing = await db.getBrandById(params.id);
  if (!existing) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand not found");
    return;
  }
  const name = params.name?.trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  const conflict = await db.getBrandByName(name);
  if (conflict && conflict.id !== params.id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand name already exists");
    return;
  }
  const connType = params.conn_type ?? "stdio";
  if (!["stdio", "http", "ws"].includes(connType)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "conn_type must be stdio|http|ws");
    return;
  }
  await db.updateBrand(params.id, {
    name,
    description: params.description ?? "",
    logo_url: params.logo_url ?? null,
    capabilities: JSON.stringify(params.capabilities ?? []),
    conn_type: connType,
    launch_cmd: params.launch_cmd ?? null,
    endpoint: params.endpoint ?? null,
    disabled: params.disabled ?? false,
  });
  await hub.reloadBrands();
  hub.broadcastAgentList();
  // launch_cmd/capabilities 可能变了：全量重推各 connector 的目标集，client 侧对账重建
  for (const connectorID of hub.connectors.keys()) {
    await hub.pushConnectorSync(connectorID);
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleBrandDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandDeleteParams>(msg);
  if (!(await db.deleteBrand(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand not found");
    return;
  }
  await hub.reloadBrands();
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 注册审批 ----

async function handleAgentApprove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentApprovalParams>(msg);
  if (!(await db.setAgentApproval(params.agent_id, "approved"))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  hub.applyAgentApproval(params.agent_id, "approved");
  if (hub.bus) {
    hub.bus.publishAgentApproval(params.agent_id, "approved")
      .catch((e) => logger.error("bus publish approval failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

async function handleAgentReject(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentApprovalParams>(msg);
  if (!(await db.setAgentApproval(params.agent_id, "rejected"))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  hub.applyAgentApproval(params.agent_id, "rejected");
  if (hub.bus) {
    hub.bus.publishAgentApproval(params.agent_id, "rejected")
      .catch((e) => logger.error("bus publish approval failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- connector 与实例分配 ----

async function handleConnectorList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const connectors: proto.ConnectorInfo[] = [...hub.connectors.values()].map((c) => ({
    id: c.connectorID ?? "",
    owner_id: c.ownerID,
    platform: c.platform,
    ip: c.ip,
    agents: [...hub.agents.values()].filter((a) => a.connectorID === c.connectorID).length,
    last_heartbeat: new Date(c.lastHeartbeat).toISOString(),
  }));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { connectors } satisfies proto.ConnectorListResult));
}

// 分配主体：admin 通道与 connector 自助通道共用（鉴权由调用方负责）
async function doAssignAgent(hub: Hub, db: Db, connector: AgentConn, params: proto.AgentAssignParams)
  : Promise<{ agent_id: string } | { error: string }> {
  const brand = hub.brands.get(params.brand_id);
  if (!brand || brand.disabled === 1) return { error: "unknown or disabled brand" };
  let agentID = params.name?.trim().replace(/[^\w-]/g, "-") ?? "";
  if (agentID !== "") {
    if (await db.getAgentRow(agentID)) return { error: "agent id already taken" };
  } else {
    // 默认 <品牌名>-<短随机>，撞库则重试
    for (;;) {
      agentID = `${brand.name.replace(/[^\w-]/g, "-")}-${crypto.randomUUID().slice(0, 6)}`;
      if (!(await db.getAgentRow(agentID))) break;
    }
  }
  await db.assignAgent({
    id: agentID,
    owner_id: connector.ownerID,
    name: agentID,
    brand_id: brand.id,
    connector_id: connector.connectorID ?? "",
  });
  await hub.pushConnectorSync(connector.connectorID ?? "");
  return { agent_id: agentID };
}

async function handleAgentAssign(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentAssignParams>(msg);
  const connector = params.connector_id ? hub.connectors.get(params.connector_id) : undefined;
  if (!connector) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "connector not online");
    return;
  }
  // connector 属主可自助分配，跨属主需 admin
  if (connector.ownerID !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  const r = await doAssignAgent(hub, db, connector, params);
  if ("error" in r) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, r.error);
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    agent_id: r.agent_id,
    status: "ok",
  } satisfies proto.AgentAssignResult));
}

// 移除主体：注意不能关 ws——connector 托管的 agent 与 connector 及其他 agent 共享连接。
// 网关侧直接注销，client 随后由 sync 对账下线（kill shim）。
async function doRemoveAgent(hub: Hub, db: Db, row: DbAgent): Promise<void> {
  await db.unassignAgent(row.id);
  hub.unregisterAgent(row.id);
  await hub.pushConnectorSync(row.connector_id ?? "");
}

async function handleAgentRemove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentRemoveParams>(msg);
  const row = await db.getAgentRow(params.agent_id);
  if (!row) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  if (!row.connector_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent is not connector-managed");
    return;
  }
  if (row.owner_id !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  await doRemoveAgent(hub, db, row);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 重启 connector 托管实例：不动 DB 行，通知 client 杀掉本地子进程并按原配置重建，
// agent_id / 会话历史 / 审批状态全部保留（适合"程序更新了、命令没变"的场景）
async function handleAgentRestart(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentRestartParams>(msg);
  const row = await db.getAgentRow(params.agent_id);
  if (!row) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  if (!row.connector_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent is not connector-managed");
    return;
  }
  if (row.owner_id !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  if (!hub.connectors.get(row.connector_id) && !hub.bus) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "connector not online");
    return;
  }
  const notif = proto.newNotification(proto.METHOD_CONNECTOR_RESTART, {
    agent_id: row.id,
  } satisfies proto.ConnectorRestartParams);
  hub.deliverToLocalConnector(row.connector_id, notif);
  if (hub.bus) {
    hub.bus.publishConnectorSync(row.connector_id, notif)
      .catch((e) => logger.error("bus publish connector restart failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", agent_id: row.id }));
}

// Agent 注册：治理模式（品牌目录非空）下必须带合法 brand_id，名称/能力以品牌行覆盖；
// client 主动注册进入 pending 待审批，页面分配的实例（agents 行已存在且 approved）直接通过。
// 一条连接可托管多个 agent（connector 模式）：每次注册创建独立 AgentConn，共享 ws。
async function handleAgentRegister(hub: Hub, base: AgentConn, params: proto.RegisterParams, msg: proto.Message): Promise<void> {
  const reject = (code: number, m: string): void => {
    sendError(base.ws, msg.id, code, m);
    base.ws.close(4001, m);
  };
  if (!params.agent_id) {
    reject(proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  let brand: DbAgentBrand | undefined;
  if (hub.governanceOn()) {
    if (!hub.db || !params.brand_id) {
      reject(proto.ERR_INVALID_PARAMS, "brand_id required (governance mode)");
      return;
    }
    brand = await hub.db.getBrandById(params.brand_id);
    if (!brand || brand.disabled === 1) {
      reject(proto.ERR_INVALID_PARAMS, "unknown or disabled brand");
      return;
    }
  }
  let approval = "approved";
  const row = hub.db ? await hub.db.getAgentRow(params.agent_id) : undefined;
  if (hub.governanceOn() && hub.db) {
    if (row?.approval_status === "rejected") {
      reject(proto.ERR_UNAUTHORIZED, "registration rejected");
      return;
    }
    if (!row) {
      await hub.db.createPendingAgent({
        id: params.agent_id,
        owner_id: base.ownerID,
        name: brand?.name ?? params.name ?? params.agent_id,
        brand_id: brand?.id ?? null,
      });
      approval = "pending";
      logger.info("agent pending approval", { agent_id: params.agent_id, owner_id: base.ownerID });
    } else if (row.approval_status === "pending") {
      approval = "pending";
    }
  }
  let brandCaps: proto.Capability[] = [];
  try { brandCaps = brand?.capabilities ? JSON.parse(brand.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
  const a: AgentConn = {
    id: params.agent_id,
    ownerID: base.ownerID, // Owner comes from token; register params owner is ignored for security.
    name: row?.name || brand?.name
      || (params.name !== "" && params.name !== undefined ? params.name : params.agent_id),
    ws: base.ws,
    capabilities: brand ? brandCaps : (params.capabilities ?? []),
    platform: params.platform ?? base.platform,
    status: proto.AGENT_STATUS_ONLINE,
    lastHeartbeat: Date.now(),
    alive: true,
    deviceKeyID: base.deviceKeyID,
    ip: base.ip,
    brandID: brand?.id ?? row?.brand_id ?? undefined,
    connectorID: row?.connector_id ?? undefined,
    approval,
  };
  // 同 id 已在其他连接上注册（僵尸实例与重启后的新实例并存）：踢掉旧连接。
  // 否则后注册者覆盖路由表，旧实例还以为自己在线，对话被路由到僵尸上。
  // 旧 client 收到 4002 必须退出而非重连，否则两个实例互踢。
  const existing = hub.agents.get(params.agent_id);
  if (existing && existing.ws !== base.ws) {
    logger.warn("agent re-registered on new connection, kicking old", { agent_id: params.agent_id });
    existing.ws.close(4002, "replaced by new connection");
  }
  hub.registerAgent(a);
  sendMsg(base.ws, proto.newResponse(msg.id ?? "", {
    status: "ok",
    server_time: proto.rfc3339Now(),
  } satisfies proto.RegisterResult));
}

export function handleAgentMessage(hub: Hub, agent: AgentConn, raw: string): void {
  let msg: proto.Message;
  try {
    msg = JSON.parse(raw) as proto.Message;
  } catch {
    sendError(agent.ws, "", proto.ERR_PARSE_ERROR, "parse error");
    return;
  }

  if (msg.jsonrpc !== proto.VERSION) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "invalid jsonrpc version");
    return;
  }

  // 配对连接（?pair=1，无凭证）只允许 connector.pair
  if (agent.pairing && msg.method !== proto.METHOD_CONNECTOR_PAIR) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "pairing connection: only connector.pair allowed");
    return;
  }

  switch (msg.method) {
    case proto.METHOD_CONNECTOR_PAIR: {
      if (!agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "connector.pair requires a ?pair=1 connection");
        break;
      }
      void handleConnectorPair(hub, agent, msg).catch((e) => {
        logger.error("connector pair failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_REGISTER: {
      const params = proto.decodeParams<proto.RegisterParams>(msg);
      void handleAgentRegister(hub, agent, params, msg).catch((e) => {
        logger.error("register failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_CONNECTOR_HELLO: {
      const params = proto.decodeParams<proto.ConnectorHelloParams>(msg);
      if (!params.connector_id) {
        sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "connector_id required");
        break;
      }
      // 同 connector_id 已在其他连接上报到（双实例并存）：踢掉旧连接，
      // 保证同一时刻只有一个实例承载该 connector 的 agent
      const prevConn = hub.connectors.get(params.connector_id);
      if (prevConn && prevConn.ws !== agent.ws) {
        logger.warn("connector re-hello on new connection, kicking old", { connector_id: params.connector_id });
        prevConn.ws.close(4002, "replaced by new connection");
      }
      agent.connectorID = params.connector_id;
      if (params.platform) agent.platform = params.platform;
      hub.registerConnector(agent);
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
      // 连接建立即推全量目标集（重连后自动恢复承载的 agent）
      void hub.pushConnectorSync(params.connector_id)
        .catch((e) => logger.error("connector sync failed", { error: String(e) }));
      break;
    }

    // ---- connector 自助（client 本地管理页经 agent 通道调用）：仅限本 connector ----

    case proto.METHOD_BRAND_LIST: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      const brands = [...hub.brands.values()].filter((b) => b.disabled !== 1).map(brandInfoOf);
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { brands } satisfies proto.BrandListResult));
      break;
    }

    case proto.METHOD_AGENT_ASSIGN: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      if (!hub.db) {
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
        break;
      }
      const params = proto.decodeParams<proto.AgentAssignParams>(msg);
      if (params.connector_id !== agent.connectorID) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "cannot assign to another connector");
        break;
      }
      const db = hub.db;
      void doAssignAgent(hub, db, agent, params).then((r) => {
        if ("error" in r) {
          sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, r.error);
        } else {
          sendMsg(agent.ws, proto.newResponse(msg.id ?? "", {
            agent_id: r.agent_id, status: "ok",
          } satisfies proto.AgentAssignResult));
        }
      }).catch((e) => {
        logger.error("connector self-assign failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_AGENT_REMOVE: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      if (!hub.db) {
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
        break;
      }
      const params = proto.decodeParams<proto.AgentRemoveParams>(msg);
      const db = hub.db;
      void (async () => {
        const row = await db.getAgentRow(params.agent_id);
        if (!row || row.connector_id !== agent.connectorID) {
          sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not hosted by this connector");
          return;
        }
        await doRemoveAgent(hub, db, row);
        sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
      })().catch((e) => {
        logger.error("connector self-remove failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_CAPABILITIES_UPDATED: {
      const params = proto.decodeParams<proto.CapabilitiesUpdatedParams>(msg);
      const a = hub.agents.get(params.agent_id || agent.id);
      if (params.session_id) {
        // C1 两级作用域：带 session_id 的是该 session/workdir 的命令与技能快照，
        // 不覆盖 Agent 全局能力（admin.agentList 仍反映全局层），仅推给页面做两层合并。
        // 可见性与 agent 事件一致：属主 + 全量 admin。
        if (a) {
          const notif = proto.newNotification(proto.METHOD_CAPABILITIES_UPDATED, {
            agent_id: a.id,
            session_id: params.session_id,
            capabilities: params.capabilities,
          } satisfies proto.CapabilitiesUpdatedParams);
          hub.forwardToUsers(a.ownerID, notif);
          for (const u of hub.users.values()) {
            if (u.isAdmin && !u.ownOnly && u.userID !== a.ownerID) hub.trySend(u.ws, notif);
          }
        }
        break;
      }
      if (a) {
        a.capabilities = params.capabilities;
        // capabilities 变化立即刷注册表：多实例下其他实例不等心跳节流（TTL/3）就能看到新命令
        hub.refreshAgentRegistry(a, true);
      }
      hub.broadcastAgentList();
      break;
    }

    case proto.METHOD_HEARTBEAT: {
      agent.lastHeartbeat = Date.now();
      const params = proto.decodeParams<proto.HeartbeatParams>(msg);
      // 一条连接可能托管多个 agent（connector 模式）：按 agent_id + 同 ws 全部续命
      const targets = new Set<AgentConn>();
      if (params.agent_id) {
        const a = hub.agents.get(params.agent_id);
        if (a) targets.add(a);
      }
      for (const a of hub.agents.values()) {
        if (a.ws === agent.ws) targets.add(a);
      }
      for (const a of targets) {
        a.lastHeartbeat = Date.now();
        hub.refreshAgentRegistry(a);
        hub.touchAgentThrottled(a);
      }
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_STATUS: {
      agent.lastHeartbeat = Date.now();
      const params = proto.decodeParams<proto.StatusParams>(msg);
      const a = hub.agents.get(params.agent_id || agent.id);
      if (a && params.status === proto.AGENT_STATUS_OFFLINE) {
        // Agent 自报下线（本地服务死亡）：注销而非只改状态。connector 连接还活着时
        // 心跳会给同 ws 的所有 agent 续命，不注销会永远显示在线
        hub.unregisterAgent(a.id);
      } else if (a && params.status && params.status !== a.status) {
        a.status = params.status;
        hub.refreshAgentRegistry(a, true);
        hub.touchAgentThrottled(a, true);
        hub.broadcastAgentList();
      }
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_AGENT_TASK_INVOKE: {
      const params = proto.decodeParams<proto.AgentTaskInvokeParams>(msg);
      void handleAgentTaskInvoke(hub, agent, msg, params).catch((e) => {
        logger.error("agent.task.invoke failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_PROGRESS: {
      const params = proto.decodeParams<proto.ProgressParams>(msg);
      const value = params.value;
      if (!value) break;
      if (!value.agent_id) value.agent_id = agent.id;
      // 多 agent 共享连接（connector）：归属/缓冲按进度里的 agent_id 定位
      const src = hub.agents.get(value.agent_id) ?? agent;
      const progress: proto.AdminProgressParams = {
        task_id: value.task_id,
        type: value.type,
        agent_id: value.agent_id,
        session_id: value.session_id,
        context_id: value.context_id,
        content: value.content,
        name: value.name,
        arguments: value.arguments,
        confirm_id: value.confirm_id,
        prompt_id: value.prompt_id,
        options: value.options,
        block_id: value.block_id,
        blocks: value.blocks,
        percentage: value.percentage,
        done: value.done,
        error: value.error,
        reason: value.reason,
      };
      const notif = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, progress);
      const ts = hub.tasks.get(value.task_id);
      if (ts?.groupID) progress.group_id = ts.groupID;
      if (ts?.parentTaskID) progress.parent_task_id = ts.parentTaskID;
      hub.forwardToUsers(src.ownerID, notif);
      // 跨属主任务（admin 操作他人 agent）：进度同时发给任务发起者
      if (ts && ts.ownerID !== src.ownerID) hub.forwardToUsers(ts.ownerID, notif);
      {
        const sessionID = value.session_id ?? ts?.sessionID ?? "";
        // confirm_cancelled 是撤销信号：标记待决 chunk 后不单独落库为 chunk
        if (value.type === proto.CHUNK_TYPE_CONFIRM_CANCELLED) {
          hub.markCancelledChunks(value.task_id, value.confirm_id ?? "",
            value.reason ?? proto.CONFIRM_CANCEL_REASON_TASK_CANCELLED);
        } else if (sessionID !== "") {
          hub.bufferProgressChunk(value.task_id, src.ownerID, src.id, sessionID,
            progress as unknown as proto.LocalAgentChunk);
        }
      }
      if (value.done || (value.error !== undefined && value.error !== "")) {
        if (ts) hub.notifySubtaskResult(value.task_id, ts, value.error);
        hub.observeTaskEnd(value.task_id, value.error !== undefined && value.error !== "" ? "failed" : "completed");
        hub.untrackTask(value.task_id);
        hub.flushTaskBuffer(value.task_id, value.error);
      }
      break;
    }

    default: {
      if (msg.id) {
        // agent 以错误响应拒绝 agent.chat：先清理关联任务条目（条目随后即被删除）
        if (msg.error) hub.cleanupRejectedTask(msg.id, msg.error.message ?? "");
        hub.forwardToPendingUser(msg.id, msg);
      }
    }
  }
}

async function handleTaskCreate(hub: Hub, user: UserConn, msg: proto.Message): Promise<void> {
  const params = proto.decodeParams<proto.TaskCreateParams>(msg);
  // 会话绑定的工作目录注入 metadata.workdir（单 agent / 群聊 / 编排子任务统一经此）；
  // 本地 Agent 自行决定如何使用，不识别则忽略，见 local-agent-interface §6.1
  if (hub.db) {
    const sess = await hub.db.getSession(user.userID, params.session_id || `${params.task_id}-session`).catch(() => undefined);
    if (sess?.workdir) {
      params.metadata = { ...(params.metadata ?? {}), workdir: sess.workdir };
    }
  }
  if (params.group_id) {
    await handleGroupTaskCreate(hub, user, msg, params);
    return;
  }
  const agent = await hub.resolveAgent(params.agent_id ?? "");
  if (!agent) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not found");
    return;
  }
  if (!hub.canManage(user, agent)) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to manage this agent");
    return;
  }
  // 待审批 agent 不接任务（仅本地连接可能处于 pending；注册表里的都已批准）
  if (hub.getAgent(params.agent_id ?? "")?.approval === "pending") {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "agent pending approval");
    return;
  }
  if (!hub.taskLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks, please slow down");
    return;
  }
  if (msg.id) hub.trackPendingRequest(msg.id, user, params.task_id);
  const sessionID = params.session_id || `${params.task_id}-session`;
  hub.trackTask(params.task_id, params.agent_id ?? "", user.userID, sessionID);
  hub.persistUserMessage(params, sessionID, user.userID);

  hub.forwardToAgent(params.agent_id ?? "", proto.newRequest(msg.id ?? "", proto.METHOD_AGENT_CHAT, {
    task_id: params.task_id,
    session_id: sessionID,
    context_id: params.context_id,
    type: params.type,
    content: params.content,
    metadata: params.metadata,
  } satisfies proto.AgentChatParams));
}

// 群上下文注入：转发 agent.chat 时在 metadata.group 带上群/成员/管理者信息，
// 让本地 Agent 无需额外配置即可感知自己是否为管理者、群里有谁、本条 @ 了谁。
// metadata 是自由扩展字段，不识别的 Agent 自动忽略，无兼容性问题。
async function buildGroupMetadata(
  hub: Hub, ownerID: string,
  group: { id: string; name: string; manager_agent_id: string | null },
  members: string[], mentions: string[], base?: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const meta: Record<string, unknown> = { ...(base ?? {}) };
  let nameOf = new Map<string, string>();
  if (hub.db) {
    const page = await hub.db.listAgentsPaged({ ownerID, limit: 1000, offset: 0 });
    nameOf = new Map(page.agents.map((a) => [a.id, a.name]));
  }
  meta.group = {
    group_id: group.id,
    group_name: group.name,
    manager_agent_id: group.manager_agent_id,
    members: members.map((id) => ({ agent_id: id, name: nameOf.get(id) ?? id })),
    mentions,
  };
  return meta;
}

// 管理者 agent 编排：父任务的处理连接经 agent 通道调用群内另一 agent（子任务）。
// 鉴权：调用连接 === 承载父任务的连接 + 群管理者 === 父任务 agent + 目标同群且属主一致；
// depth 硬限 1（编排产生的子任务不能再发起编排）。多实例下任务态在创建实例，
// 跨实例 invoke 会得到 parent task not found（编排要求管理者与任务同实例）。
async function handleAgentTaskInvoke(
  hub: Hub, agent: AgentConn, msg: proto.Message, params: proto.AgentTaskInvokeParams,
): Promise<void> {
  if (!hub.db) {
    sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  const ts = hub.tasks.get(params.parent_task_id);
  if (!ts) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "parent task not found");
    return;
  }
  if (ts.depth >= 1) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "nested orchestration not allowed");
    return;
  }
  const parentConn = hub.agents.get(ts.agentID);
  if (!parentConn || parentConn.ws !== agent.ws) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "parent task not served by this connection");
    return;
  }
  const db = hub.db;
  const group = await db.getGroup(ts.ownerID, params.group_id);
  if (!group || group.manager_agent_id !== ts.agentID) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "caller is not the group manager");
    return;
  }
  const members = await db.listGroupMembers(group.id);
  if (params.target_agent_id === ts.agentID || !members.includes(params.target_agent_id)) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "target agent not in group");
    return;
  }
  const target = await hub.resolveAgent(params.target_agent_id);
  if (!target || target.ownerID !== ts.ownerID) {
    sendError(agent.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "target agent not found");
    return;
  }
  if (hub.getAgent(params.target_agent_id)?.approval === "pending") {
    sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "target agent pending approval");
    return;
  }
  if (!hub.taskLimiter.allow(ts.ownerID)) {
    sendError(agent.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks");
    return;
  }
  const childTaskID = `${params.parent_task_id}@${crypto.randomUUID().slice(0, 8)}`;
  hub.trackTask(childTaskID, params.target_agent_id, ts.ownerID, ts.sessionID, {
    groupID: group.id,
    parentTaskID: params.parent_task_id,
    invokerAgentID: ts.agentID,
    depth: ts.depth + 1,
  });
  const childMeta = await buildGroupMetadata(
    hub, ts.ownerID, group, members, [params.target_agent_id], params.metadata);
  // 子任务复用父任务会话：会话绑定的 workdir 同样注入（manager 未显式携带时兜底）
  const childSession = ts.sessionID
    ? await hub.db!.getSession(ts.ownerID, ts.sessionID).catch(() => undefined)
    : undefined;
  if (childSession?.workdir && childMeta.workdir === undefined) {
    childMeta.workdir = childSession.workdir;
  }
  hub.forwardToAgent(params.target_agent_id, proto.newRequest("", proto.METHOD_AGENT_CHAT, {
    task_id: childTaskID,
    session_id: ts.sessionID,
    type: params.type,
    content: params.content,
    metadata: childMeta,
  } satisfies proto.AgentChatParams));
  sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { task_id: childTaskID, status: "dispatched" } satisfies proto.AgentTaskInvokeResult));
}

// 群聊路径：@提及 路由（mentions 空则拒绝，保证"默认不触发"），多目标 fan-out 派生 task_id（<tid>#<n>）。
// 网关立即应答 task.create（不等 agent），避免多个 agent 对同一 msg.id 重复响应。
async function handleGroupTaskCreate(
  hub: Hub, user: UserConn, msg: proto.Message, params: proto.TaskCreateParams,
): Promise<void> {
  if (!hub.db) {
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  const db = hub.db;
  const group = await db.getGroup(user.userID, params.group_id!);
  if (!group) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  const members = await db.listGroupMembers(group.id);
  const mentions = [...new Set(params.mentions ?? [])];
  if (mentions.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "mentions required in group chat (@agent or @all)");
    return;
  }
  const targets = mentions.includes("all") ? members : mentions;
  if (targets.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group has no member agents");
    return;
  }
  if (targets.length > MAX_GROUP_FANOUT) {
    sendError(user.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, `too many targets (max ${MAX_GROUP_FANOUT})`);
    return;
  }
  for (const m of targets) {
    if (!members.includes(m)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `agent not in group: ${m}`);
      return;
    }
  }
  if (!hub.taskLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks, please slow down");
    return;
  }
  // 离线目标不派发（forwardToAgent 只会静默丢弃）：跳过并在响应中告知
  const online: string[] = [];
  const skipped: string[] = [];
  for (const t of targets) {
    if (await hub.resolveAgent(t)) online.push(t);
    else skipped.push(t);
  }
  if (online.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, `all mentioned agents offline: ${skipped.join(", ")}`);
    return;
  }
  const sessionID = params.session_id || `${params.task_id}-session`;
  // 群会话标识 group:<gid>；一条 user 消息只落一次
  hub.persistUserMessage({ ...params, agent_id: `group:${group.id}` }, sessionID, user.userID);

  const taskIDs: string[] = [];
  const groupMeta = await buildGroupMetadata(
    hub, user.userID, group, members, online, params.metadata);
  online.forEach((target, i) => {
    const taskID = online.length === 1 ? params.task_id : `${params.task_id}#${i}`;
    taskIDs.push(taskID);
    hub.trackTask(taskID, target, user.userID, sessionID, { groupID: group.id });
    hub.forwardToAgent(target, proto.newRequest("", proto.METHOD_AGENT_CHAT, {
      task_id: taskID,
      session_id: sessionID,
      context_id: params.context_id,
      type: params.type,
      content: params.content,
      metadata: groupMeta,
    } satisfies proto.AgentChatParams));
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    task_id: params.task_id,
    status: "accepted",
    group_id: group.id,
    task_ids: taskIDs,
    ...(skipped.length ? { skipped_offline: skipped } : {}),
  }));
}

// 群级取消：按基任务 id 收敛 fan-out 派生任务（<tid>#n）与编排子任务（parent 指向本批），
// 逐个下发 agent.cancel；任务清理仍由 agent 的 done 进度驱动（与单 agent 取消一致）
function handleGroupTaskCancel(hub: Hub, user: UserConn, msg: proto.Message, params: proto.TaskCancelParams): void {
  const prefix = `${params.task_id}#`;
  const matches: Array<[string, TaskState]> = [];
  for (const [tid, ts] of hub.tasks) {
    const inFamily = tid === params.task_id || tid.startsWith(prefix)
      || (ts.parentTaskID !== undefined && (ts.parentTaskID === params.task_id || ts.parentTaskID.startsWith(prefix)));
    if (inFamily && (ts.ownerID === user.userID || user.isAdmin)) matches.push([tid, ts]);
  }
  for (const [tid, ts] of matches) {
    hub.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
      task_id: tid,
      session_id: ts.sessionID || undefined,
    } satisfies proto.AgentCancelParams));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { task_id: params.task_id, status: "cancelling" } satisfies proto.TaskCancelResult));
}

// cancel/respond 的公共转发逻辑：agent_id 为空时广播给该用户名下所有 agent
async function handleTaskForward(
  hub: Hub, user: UserConn, msg: proto.Message, method: string,
  params: object, agentID?: string,
): Promise<void> {
  // 登记 pendingRequest，让 client.ts 的响应能通过 forwardToPendingUser 路由回 browser。
  // task.cancel 通常不期待有意义的响应，但 task.respond 必须把 result 透传回去（rule ③）。
  if (msg.id) hub.trackPendingRequest(msg.id, user);
  const req = proto.newRequest(msg.id ?? "", method, params);
  const target = agentID ?? (params as { agent_id?: string }).agent_id ?? "";
  if (target !== "") {
    const agent = await hub.resolveAgent(target);
    if (!agent) {
      sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not found");
      return;
    }
    if (!hub.canManage(user, agent)) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to manage this agent");
      return;
    }
    hub.forwardToAgent(target, req);
    return;
  }
  for (const id of await hub.resolveOwnerAgentIDs(user.userID)) {
    hub.forwardToAgent(id, req);
  }
}

export function handleUserMessage(hub: Hub, user: UserConn, raw: string): void {  let msg: proto.Message;
  try {
    msg = JSON.parse(raw) as proto.Message;
  } catch {
    sendError(user.ws, "", proto.ERR_PARSE_ERROR, "parse error");
    return;
  }
  user.lastHeartbeat = Date.now();

  if (msg.jsonrpc !== proto.VERSION) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_REQUEST, "invalid jsonrpc version");
    return;
  }

  switch (msg.method) {
    case proto.METHOD_TASK_CREATE: {
      void handleTaskCreate(hub, user, msg).catch((e) => {
        logger.error("task.create failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_TASK_CANCEL: {
      const cancelParams = proto.decodeParams<proto.TaskCancelParams>(msg);
      if (cancelParams.group_id) {
        handleGroupTaskCancel(hub, user, msg, cancelParams);
        break;
      }
      // C6：单 agent 任务取消同样按 parentTaskID 级联子任务（群路径在 handleGroupTaskCancel 内收敛）。
      // 属主判定与群路径一致：本人或 admin 可级联他人任务的子任务。
      if (cancelParams.task_id) {
        hub.cascadeCancelSubtasks(cancelParams.task_id, (ts) => ts.ownerID === user.userID || user.isAdmin);
      }
      void handleTaskForward(hub, user, msg, proto.METHOD_AGENT_CANCEL, cancelParams).catch((e) => {
        logger.error("task.cancel failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_TASK_RESPOND: {
      const params = proto.decodeParams<proto.TaskRespondParams>(msg);
      hub.markRespondedChunk(params.task_id, params.confirm_id ?? "", params.prompt_id ?? "", params.block_id ?? "", params.response);
      void handleTaskForward(hub, user, msg, proto.METHOD_AGENT_RESPOND, {
        task_id: params.task_id,
        session_id: params.session_id,
        confirm_id: params.confirm_id,
        prompt_id: params.prompt_id,
        block_id: params.block_id,
        action_id: params.action_id,
        response: params.response,
      } satisfies proto.AgentRespondParams, params.agent_id).catch((e) => {
        logger.error("task.respond failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_SESSION_LIST:
      withDb(hub, user, msg, (db) => handleSessionList(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_CREATE:
      withDb(hub, user, msg, (db) => handleSessionCreate(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_RENAME:
      withDb(hub, user, msg, (db) => handleSessionRename(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_SET_WORKDIR:
      withDb(hub, user, msg, (db) => handleSessionSetWorkdir(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_DELETE:
      withDb(hub, user, msg, (db) => handleSessionDelete(hub, user, msg, db));
      break;

    case proto.METHOD_MESSAGE_LIST:
      withDb(hub, user, msg, (db) => handleMessageList(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_CREATE:
      withDb(hub, user, msg, (db) => handleGroupCreate(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_LIST:
      withDb(hub, user, msg, (db) => handleGroupList(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_DETAIL:
      withDb(hub, user, msg, (db) => handleGroupDetail(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_ADD:
      withDb(hub, user, msg, (db) => handleGroupAdd(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_REMOVE:
      withDb(hub, user, msg, (db) => handleGroupRemove(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_RENAME:
      withDb(hub, user, msg, (db) => handleGroupRename(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_SET_MANAGER:
      withDb(hub, user, msg, (db) => handleGroupSetManager(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_DELETE:
      withDb(hub, user, msg, (db) => handleGroupDelete(hub, user, msg, db));
      break;

    case proto.METHOD_USER_LIST:
      withDb(hub, user, msg, (db) => handleUserList(hub, user, msg, db));
      break;

    case proto.METHOD_USER_CREATE:
      withDb(hub, user, msg, (db) => handleUserCreate(hub, user, msg, db));
      break;

    case proto.METHOD_USER_DISABLE:
      withDb(hub, user, msg, (db) => handleUserDisable(hub, user, msg, db));
      break;

    case proto.METHOD_USER_RESET_PASSWORD:
      withDb(hub, user, msg, (db) => handleUserResetPassword(hub, user, msg, db));
      break;

    case proto.METHOD_USER_CHANGE_PASSWORD:
      withDb(hub, user, msg, (db) => handleUserChangePassword(hub, user, msg, db));
      break;

    case proto.METHOD_USER_SET_ROLE:
      withDb(hub, user, msg, (db) => handleUserSetRole(hub, user, msg, db));
      break;

    case proto.METHOD_USER_DELETE:
      withDb(hub, user, msg, (db) => handleUserDelete(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_LIST:
      withDb(hub, user, msg, (db) => handleAgentList(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_DISCONNECT:
      withDb(hub, user, msg, (db) => handleAgentDisconnect(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REASSIGN:
      withDb(hub, user, msg, (db) => handleAgentReassign(hub, user, msg, db));
      break;

    case proto.METHOD_ADMIN_OVERVIEW:
      withDb(hub, user, msg, (db) => handleAdminOverview(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_CREATE:
      withDb(hub, user, msg, (db) => handleDeviceKeyCreate(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_LIST:
      withDb(hub, user, msg, (db) => handleDeviceKeyList(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_REVOKE:
      withDb(hub, user, msg, (db) => handleDeviceKeyRevoke(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_LIST:
      withDb(hub, user, msg, (db) => handleBrandList(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_CREATE:
      withDb(hub, user, msg, (db) => handleBrandCreate(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_UPDATE:
      withDb(hub, user, msg, (db) => handleBrandUpdate(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_DELETE:
      withDb(hub, user, msg, (db) => handleBrandDelete(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_APPROVE:
      withDb(hub, user, msg, (db) => handleAgentApprove(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REJECT:
      withDb(hub, user, msg, (db) => handleAgentReject(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_LIST:
      withDb(hub, user, msg, (db) => handleConnectorList(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_ASSIGN:
      withDb(hub, user, msg, (db) => handleAgentAssign(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REMOVE:
      withDb(hub, user, msg, (db) => handleAgentRemove(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_RESTART:
      withDb(hub, user, msg, (db) => handleAgentRestart(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_SET_NICKNAME:
      withDb(hub, user, msg, (db) => handleAgentSetNickname(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_CREATE:
      withDb(hub, user, msg, (db) => handlePairingCreate(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_LIST:
      withDb(hub, user, msg, (db) => handlePairingList(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_DELETE:
      withDb(hub, user, msg, (db) => handlePairingDelete(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_PENDING_LIST:
      withDb(hub, user, msg, (db) => handleConnectorPendingList(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_APPROVE:
      withDb(hub, user, msg, (db) => handleConnectorApprove(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_REJECT:
      withDb(hub, user, msg, (db) => handleConnectorReject(hub, user, msg, db));
      break;

    default:
      sendError(user.ws, msg.id, proto.ERR_METHOD_NOT_FOUND, `method not found: ${msg.method}`);
  }
}

// Watches a connection with periodic pings; terminates after consecutive
// missed pongs. 不再单轮 miss 即杀：系统睡眠唤醒时 ticker 先于对端补 pong 触发，
// 会把活连接误杀——连续 2 轮未回且距最近 pong 超 75s 才判死。
function watchPong(ws: WebSocket, conn: { alive: boolean }, onPong?: () => void): NodeJS.Timeout {
  let lastPongAt = Date.now();
  let misses = 0;
  ws.on("pong", () => {
    conn.alive = true;
    misses = 0;
    lastPongAt = Date.now();
    onPong?.();
  });
  const ticker = setInterval(() => {
    if (conn.alive) {
      conn.alive = false;
      misses = 0;
      ws.ping();
      return;
    }
    misses++;
    if (misses >= 2 && Date.now() - lastPongAt > 75_000) {
      ws.terminate();
      return;
    }
    ws.ping();
  }, 30_000);
  ticker.unref();
  return ticker;
}

export interface GatewayConfig {
  addr: string;
  logLevel: string;
  agentTimeoutMs: number;
  userTimeoutMs: number;
  taskTimeoutMs: number;
  databaseURL: string;
  jwtSecret: string;
  jwtTtlMs: number;
  adminPassword: string;
  redisURL: string;
  redisPrefix: string;
  instanceID: string;
  trustProxy: boolean; // 前面有可信反代时才信任 X-Forwarded-For
  attachDir: string;
  attachQuotaMb: number;
  retentionDays: number;
  s3Endpoint: string;
  s3Region: string;
  s3Bucket: string;
  s3AccessKey: string;
  s3SecretKey: string;
  s3PublicURL: string;
  // OIDC 统一认证（四项全配才启用）
  oidcIssuer: string;
  oidcClientID: string;
  oidcClientSecret: string;
  oidcRedirectURL: string;
  oidcEmployeeClaim: string;
}

export function loadGatewayConfig(): GatewayConfig {
  const specs = [
    { name: "addr", type: "string" as const, default: envString("AGENT_MANAGE_ADDR", ":8080") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "agent-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_AGENT_TIMEOUT", 90_000)) },
    { name: "user-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_USER_TIMEOUT", 120_000)) },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 7_200_000)) },
    { name: "database-url", type: "string" as const, default: envString("AGENT_MANAGE_DATABASE_URL", "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix") },
    { name: "jwt-secret", type: "string" as const, default: envString("AGENT_MANAGE_JWT_SECRET", "") },
    { name: "jwt-ttl", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_JWT_TTL", 7 * 86400_000)) },
    { name: "admin-password", type: "string" as const, default: envString("AGENT_MANAGE_ADMIN_PASSWORD", "admin123") },
    { name: "redis-url", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_URL", "") },
    { name: "redis-prefix", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_PREFIX", "ywm") },
    { name: "instance-id", type: "string" as const, default: envString("AGENT_MANAGE_INSTANCE_ID", crypto.randomBytes(6).toString("hex")) },
    { name: "trust-proxy", type: "string" as const, default: envString("AGENT_MANAGE_TRUST_PROXY", "") },
    { name: "attach-dir", type: "string" as const, default: envString("AGENT_MANAGE_ATTACH_DIR", "data/attachments") },
    { name: "attach-quota-mb", type: "string" as const, default: envString("AGENT_MANAGE_ATTACH_QUOTA_MB", "0") },
    { name: "retention-days", type: "string" as const, default: envString("AGENT_MANAGE_RETENTION_DAYS", "0") },
    { name: "s3-endpoint", type: "string" as const, default: envString("AGENT_MANAGE_S3_ENDPOINT", "") },
    { name: "s3-region", type: "string" as const, default: envString("AGENT_MANAGE_S3_REGION", "us-east-1") },
    { name: "s3-bucket", type: "string" as const, default: envString("AGENT_MANAGE_S3_BUCKET", "ywmatrix") },
    { name: "s3-access-key", type: "string" as const, default: envString("AGENT_MANAGE_S3_ACCESS_KEY", "minioadmin") },
    { name: "s3-secret-key", type: "string" as const, default: envString("AGENT_MANAGE_S3_SECRET_KEY", "minioadmin") },
    { name: "s3-public-url", type: "string" as const, default: envString("AGENT_MANAGE_S3_PUBLIC_URL", "") },
    { name: "oidc-issuer", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_ISSUER", "") },
    { name: "oidc-client-id", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_CLIENT_ID", "") },
    { name: "oidc-client-secret", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_CLIENT_SECRET", "") },
    { name: "oidc-redirect-url", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_REDIRECT_URL", "") },
    { name: "oidc-employee-claim", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_EMPLOYEE_CLAIM", "employee_id") },
  ];
  const values = parseFlags(specs);
  const toMs = (v: string, def: number): number => {
    const asNum = Number(v);
    if (v !== "" && !Number.isNaN(asNum)) return asNum;
    const parsed = parseDurationMs(v);
    return parsed !== undefined ? parsed : def;
  };
  return {
    addr: values["addr"],
    logLevel: values["log-level"],
    agentTimeoutMs: toMs(values["agent-timeout"], 90_000),
    userTimeoutMs: toMs(values["user-timeout"], 120_000),
    taskTimeoutMs: toMs(values["task-timeout"], 7_200_000),
    databaseURL: values["database-url"],
    jwtSecret: values["jwt-secret"],
    jwtTtlMs: toMs(values["jwt-ttl"], 7 * 86400_000),
    adminPassword: values["admin-password"],
    redisURL: values["redis-url"],
    redisPrefix: values["redis-prefix"],
    instanceID: values["instance-id"],
    trustProxy: /^(1|true|yes)$/i.test(values["trust-proxy"]),
    s3Endpoint: values["s3-endpoint"],
    s3Region: values["s3-region"],
    s3Bucket: values["s3-bucket"],
    s3AccessKey: values["s3-access-key"],
    s3SecretKey: values["s3-secret-key"],
    s3PublicURL: values["s3-public-url"],
    attachDir: values["attach-dir"],
    attachQuotaMb: Number(values["attach-quota-mb"]) || 0,
    retentionDays: Number(values["retention-days"]) || 0,
    oidcIssuer: values["oidc-issuer"],
    oidcClientID: values["oidc-client-id"],
    oidcClientSecret: values["oidc-client-secret"],
    oidcRedirectURL: values["oidc-redirect-url"],
    oidcEmployeeClaim: values["oidc-employee-claim"],
  };
}

function escapeHtmlText(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function readBody(req: http.IncomingMessage, limit = 64 * 1024): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > limit) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString()));
    req.on("error", reject);
  });
}

export async function createGatewayServer(cfg: GatewayConfig, staticFile: string, db?: Db, attachments?: AttachmentStore) {
  const hub = new Hub(cfg.agentTimeoutMs, cfg.userTimeoutMs, cfg.taskTimeoutMs);
  hub.db = db;
  hub.attachments = attachments;
  if (db) await hub.reloadBrands();
  const loginLimiter = new RateLimiter(10, 60_000); // 每 IP 每分钟 10 次登录尝试
  const uploadLimiter = new RateLimiter(20, 60_000); // 每用户每分钟 20 次上传
  // 用户不存在时也跑一次 scrypt，拉齐登录接口时序，防用户名枚举
  const dummyPasswordHash = hashPassword(crypto.randomBytes(16).toString("hex"));

  // OIDC 四项全配才启用；未启用时 /auth/oidc/* 返回 404
  const oidc = (cfg.oidcIssuer && cfg.oidcClientID && cfg.oidcClientSecret && cfg.oidcRedirectURL)
    ? new OIDCProvider({
        issuer: cfg.oidcIssuer,
        clientID: cfg.oidcClientID,
        clientSecret: cfg.oidcClientSecret,
        redirectURL: cfg.oidcRedirectURL,
        employeeClaim: cfg.oidcEmployeeClaim || "employee_id",
      }, cfg.jwtSecret)
    : undefined;
  if (oidc) logger.info("oidc enabled", { issuer: cfg.oidcIssuer, client_id: cfg.oidcClientID });

  let bus: Bus | undefined;
  if (cfg.redisURL !== "") {
    bus = new Bus(cfg.redisURL, cfg.instanceID, cfg.agentTimeoutMs * 2, {
      onAgentMessage: (agentID, msg) => hub.deliverToLocalAgent(agentID, msg),
      onUserMessage: (ownerID, msg) => {
        hub.deliverToLocalUsers(ownerID, msg);
        // 任务终结通知可能跨实例到达，顺手清理本实例的任务计时器
        if (msg.method === proto.METHOD_ADMIN_PROGRESS) {
          const p = (msg.params ?? {}) as proto.AdminProgressParams;
          if (p.task_id && (p.done || (p.error !== undefined && p.error !== ""))) {
            hub.untrackTask(p.task_id);
          }
        }
      },
      onPendingResponse: (reqID, msg) => { hub.deliverToLocalPending(reqID, msg); },
      onAgentsChanged: () => hub.broadcastAgentList(),
      onKick: (userID, deviceKeyID, reason) => hub.kickLocal(userID, deviceKeyID, reason),
      onConnectorSync: (connectorID, msg) => hub.deliverToLocalConnector(connectorID, msg),
      onAgentApproval: (agentID, status) => hub.applyAgentApproval(agentID, status),
    }, cfg.redisPrefix);
    await bus.start();
    hub.bus = bus;
    logger.info("redis bus connected", { instance_id: cfg.instanceID });
  }

  const server = http.createServer({
    // 显式超时，防慢速请求占住连接（与 Node 18+ 默认值一致，写死防升级漂移）
    headersTimeout: 60_000,
    requestTimeout: 300_000,
  }, (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/healthz" && req.method === "GET") {
      void (async () => {
        const dbOk = hub.draining ? "draining" : hub.db ? await hub.db.ping() : "disabled";
        const redisOk = hub.bus ? await hub.bus.ping() : "disabled";
        const healthy = !hub.draining && dbOk !== false && redisOk !== false;
        res.writeHead(healthy ? 200 : 503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: hub.draining ? "draining" : healthy ? "ok" : "degraded", db: dbOk, redis: redisOk, uptime_s: Math.floor(process.uptime()) }));
      })().catch(() => res.writeHead(503).end(JSON.stringify({ status: "error" })));
      return;
    }
    if (url.pathname === "/metrics" && req.method === "GET") {
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(hub.metrics.render([
        ["ywm_agents_connected", "Agents currently connected to this instance", hub.agents.size],
        ["ywm_users_connected", "Admin page connections on this instance", hub.users.size],
        ["ywm_tasks_active", "Tasks in flight", hub.tasks.size],
        ["ywm_pending_requests", "RPC requests awaiting agent response", hub.pendingRequests.size],
      ]));
      return;
    }
    if (url.pathname.startsWith("/files/") && req.method === "GET") {
      // 本地盘附件回源（URL 含 UUID，与 S3 匿名读策略同级）
      void (async () => {
        if (!(attachments instanceof LocalAttachmentStore)) {
          res.writeHead(404).end("not found");
          return;
        }
        const key = decodeURIComponent(url.pathname.slice("/files/".length));
        const obj = await attachments.get(key);
        if (!obj) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, {
          "Content-Type": obj.mime,
          "Content-Length": obj.body.length,
          "Cache-Control": "public, max-age=31536000, immutable",
          "Content-Security-Policy": "sandbox",
        });
        res.end(obj.body);
      })().catch(() => res.writeHead(500).end("internal error"));
      return;
    }
    // 静态文件内存缓存：文件就几个，首次请求后不再读盘（重启进程即失效，无需失效机制）
    const fileCache = new Map<string, Buffer | null>();
    const readCached = (file: string, cb: (data: Buffer | null) => void) => {
      const hit = fileCache.get(file);
      if (hit !== undefined || fileCache.has(file)) {
        cb(hit ?? null);
        return;
      }
      fs.readFile(file, (err, data) => {
        fileCache.set(file, err ? null : data);
        cb(err ? null : data);
      });
    };
    // ETag（内容哈希，同内容跨重启稳定）+ If-None-Match → 304：
    // no-cache 策略下重复导航不再重传 HTML/CSS，只回"没变"
    const fileEtags = new Map<string, string>();
    const etagOf = (file: string, data: Buffer): string => {
      let et = fileEtags.get(file);
      if (!et) {
        et = `"${crypto.createHash("sha1").update(data).digest("base64url").slice(0, 20)}"`;
        fileEtags.set(file, et);
      }
      return et;
    };
    const normEtag = (s: string): string => (s.startsWith("W/") ? s.slice(2) : s);
    const isNotModified = (file: string, data: Buffer): boolean => {
      const inm = req.headers["if-none-match"];
      return typeof inm === "string" && normEtag(inm) === normEtag(etagOf(file, data));
    };
    const serveHtml = (file: string) => {
      readCached(file, (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        const et = etagOf(file, data);
        if (isNotModified(file, data)) {
          res.writeHead(304, { ETag: et });
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache", ETag: et });
        res.end(data);
      });
    };
    if (url.pathname === "/") {
      serveHtml(staticFile);
      return;
    }
    if (url.pathname === "/admin" || url.pathname === "/admin.html") {
      // 管理后台是独立页面；鉴权在页面内由 JWT+role 完成
      serveHtml(path.resolve(path.dirname(staticFile), "admin.html"));
      return;
    }
    if (url.pathname.startsWith("/static/") && req.method === "GET") {
      // 共享静态资源（如 shared.css）；限制在 static 目录内防路径穿越
      const STATIC_MIME: Record<string, string> = { ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webmanifest": "application/manifest+json", ".webp": "image/webp", ".gif": "image/gif", ".ico": "image/x-icon", ".woff": "font/woff", ".woff2": "font/woff2" };
      const name = url.pathname.slice("/static/".length);
      const file = path.resolve(path.dirname(staticFile), name);
      if (name.includes("/") || !file.startsWith(path.dirname(staticFile) + path.sep)) {
        res.writeHead(404).end("not found");
        return;
      }
      readCached(file, (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        const et = etagOf(file, data);
        if (isNotModified(file, data)) {
          res.writeHead(304, { ETag: et });
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": STATIC_MIME[path.extname(file)] ?? "application/octet-stream", "Cache-Control": "no-cache", ETag: et });
        res.end(data);
      });
      return;
    }
    // PWA Service Worker 必须落在根 scope 才能控制整站，所以从根路径服务
    // static/sw.js，并用 Service-Worker-Allowed 放开 scope
    if (url.pathname === "/sw.js" && req.method === "GET") {
      readCached(path.resolve(path.dirname(staticFile), "sw.js"), (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, {
          "Content-Type": "text/javascript; charset=utf-8",
          "Cache-Control": "no-cache",
          "Service-Worker-Allowed": "/"
        });
        res.end(data);
      });
      return;
    }
    if (url.pathname === "/auth/config" && req.method === "GET") {
      // 登录页据此决定是否展示统一认证入口
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ oidc: oidc !== undefined }));
      return;
    }
    if (url.pathname === "/auth/oidc/login" && req.method === "GET") {
      if (!oidc) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        const authURL = await oidc.buildAuthURL();
        res.writeHead(302, { Location: authURL });
        res.end();
      })().catch((e) => {
        logger.error("oidc login failed", { error: String(e) });
        res.writeHead(502, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("统一认证服务暂时不可用");
      });
      return;
    }
    if (url.pathname === "/auth/oidc/callback" && req.method === "GET") {
      if (!oidc) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        const fail = (message: string) => {
          res.writeHead(400, { "Content-Type": "text/html; charset=utf-8" });
          res.end(`<!doctype html><meta charset="utf-8"><title>登录失败</title><p>统一认证登录失败：${escapeHtmlText(message)}</p><p><a href="/">返回登录页</a></p>`);
        };
        const code = url.searchParams.get("code") ?? "";
        const state = url.searchParams.get("state") ?? "";
        if (!code || !state) {
          fail("缺少 code/state 参数");
          return;
        }
        if (!hub.db) {
          fail("存储未配置");
          return;
        }
        let identity;
        try {
          identity = await oidc.authenticate(code, state);
        } catch (e) {
          logger.warn("oidc authenticate failed", { error: String(e) });
          fail(e instanceof Error ? e.message : String(e));
          return;
        }
        // 按工号关联账号；首次登录自动建号（用户名冲突时追加后缀）
        let user = await hub.db.getUserByEmployeeID(identity.employeeID);
        if (!user) {
          // name 与 employee_id 均有唯一约束，并发首次登录时撞哪边处理哪边：
          // 撞 employee_id 直接复用已建账号，撞 name 换后缀重试
          for (let attempt = 0; attempt < 5 && !user; attempt++) {
            let name = identity.displayName;
            for (let i = 0; await hub.db.getUserByName(name); i++) {
              name = `${identity.displayName}-${i + 2}`;
            }
            const candidate = {
              id: "u-" + crypto.randomUUID(),
              name,
              // OIDC 账号无本地密码：随机哈希占位，密码登录永远不匹配
              password_hash: hashPassword(crypto.randomBytes(32).toString("hex")),
              role: "user",
              disabled: 0,
              created_at: Date.now(),
              last_login_at: null,
              employee_id: identity.employeeID,
              display_name: identity.displayName,
            };
            try {
              await hub.db.createUser(candidate);
              user = candidate;
              logger.info("oidc user provisioned", { user_id: candidate.id, name: candidate.name, employee_id: identity.employeeID });
            } catch (e) {
              if ((e as { errno?: number }).errno !== 1062) throw e;
              user = await hub.db.getUserByEmployeeID(identity.employeeID);
            }
          }
          if (!user) throw new Error("oidc user provisioning failed");
        }
        if (user.disabled === 1) {
          fail("账号已被禁用，请联系管理员");
          return;
        }
        hub.db.touchLastLogin(user.id).catch(() => {});
        const token = signJwt({ sub: user.id, name: user.name }, cfg.jwtSecret, cfg.jwtTtlMs);
        // 回调页与 SPA 同源，直接写 localStorage 后跳转（该页无 JS 回写，不会冲突）
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(`<!doctype html><meta charset="utf-8"><title>登录成功</title><p>登录成功，正在跳转…</p><script>
try {
  var s = JSON.parse(localStorage.getItem('agent_manage_v1') || '{}');
  s.token = ${JSON.stringify(token)};
  s.user = ${JSON.stringify({ id: user.id, name: user.name, role: user.role })};
  localStorage.setItem('agent_manage_v1', JSON.stringify(s));
} catch (e) {}
location.replace('/');
</script>`);
      })().catch((e) => {
        logger.error("oidc callback failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("internal error");
      });
      return;
    }
    if (url.pathname === "/auth/login" && req.method === "POST") {
      void (async () => {
        const ip = clientIp(req.headers, req.socket.remoteAddress, cfg.trustProxy);
        if (!loginLimiter.allow(ip)) {
          res.writeHead(429, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "too many login attempts, try again later" }));
          return;
        }
        if (!db) {
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "storage not configured" }));
          return;
        }
        let body: { name?: string; password?: string };
        try {
          body = JSON.parse(await readBody(req)) as typeof body;
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid json" }));
          return;
        }
        const fail = () => {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid credentials" }));
        };
        const user = body.name ? await db.getUserByName(body.name) : undefined;
        // 无论用户是否存在都执行一次 scrypt，拉齐响应时序（|| 短路会前功尽弃）
        const passwordOk = verifyPassword(body.password ?? "", user?.password_hash ?? dummyPasswordHash);
        if (!user || user.disabled === 1 || !body.password || !passwordOk) {
          fail();
          return;
        }
        const token = signJwt({ sub: user.id, name: user.name }, cfg.jwtSecret, cfg.jwtTtlMs);
        db.touchLastLogin(user.id).catch((e) => logger.warn("touch last login failed", { error: String(e) }));
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ token, user: { id: user.id, name: user.name, role: user.role } }));
      })().catch((e) => {
        logger.error("login failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }
    // 轻量 token 校验：前端启动时先验一次，失效 token 直接清掉停在登录页，
    // 避免乐观进主界面后 WS 三次 401 重试又弹回登录页的闪进闪回
    if (url.pathname === "/auth/me" && req.method === "GET") {
      void (async () => {
        const bearer = req.headers.authorization?.startsWith("Bearer ")
          ? req.headers.authorization.slice(7)
          : "";
        const claims = bearer ? verifyJwt(bearer, cfg.jwtSecret) : undefined;
        const user = claims && db ? await db.getUserById(claims.sub).catch(() => undefined) : undefined;
        if (!claims || !user || user.disabled === 1) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ user: { id: user.id, name: user.name, role: user.role } }));
      })().catch((e) => {
        logger.error("auth/me failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }
    if (url.pathname === "/attachments" && req.method === "POST") {
      void (async () => {
        if (!attachments) {
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "attachment store not configured" }));
          return;
        }
        // 仅接受 Authorization 头；URL query 传 token 会被反代日志/浏览器历史记录
        const bearer = req.headers.authorization?.startsWith("Bearer ")
          ? req.headers.authorization.slice(7)
          : "";
        const claims = bearer ? verifyJwt(bearer, cfg.jwtSecret) : undefined;
        if (!claims) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        if (!uploadLimiter.allow(claims.sub)) {
          res.writeHead(429, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "upload too frequent" }));
          return;
        }
        let body: { name?: string; mime?: string; data?: string };
        try {
          body = JSON.parse(await readBody(req, 32 * 1024 * 1024)) as typeof body;
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid json or body too large" }));
          return;
        }
        if (!body.data) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "missing data" }));
          return;
        }
        const b64 = body.data.includes(",") ? body.data.slice(body.data.indexOf(",") + 1) : body.data;
        const buf = Buffer.from(b64, "base64");
        if (buf.length === 0 || buf.length > 20 * 1024 * 1024) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "empty or exceeds 20MB" }));
          return;
        }
        // 按用户配额（仅本地盘模式；S3 模式由 bucket 策略/生命周期管理）
        if (cfg.attachQuotaMb > 0 && attachments instanceof LocalAttachmentStore) {
          const used = await attachments.usage(`attachments/${claims.sub}`);
          if (used + buf.length > cfg.attachQuotaMb * 1024 * 1024) {
            res.writeHead(429, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: "attachment quota exceeded" }));
            return;
          }
        }
        const mime = body.mime ?? "application/octet-stream";
        const key = `attachments/${claims.sub}/${crypto.randomUUID()}/${sanitizeFileName(body.name ?? "file")}`;
        const fileUrl = await attachments.put(key, buf, mime);
        hub.metrics.inc("ywm_attachments_uploaded_total");
        hub.metrics.inc("ywm_attachment_bytes_total", buf.length);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ url: fileUrl, name: body.name ?? "file", mime, size: buf.length }));
      })().catch((e) => {
        logger.error("attachment upload failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }
    res.writeHead(404).end("not found");
  });

  // maxPayload 限制单帧大小（ws 默认 100MiB 太大）；附件走 HTTP，WS 上都是 JSON 控制消息
  const wss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });

  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const token = url.searchParams.get("token") ?? "";
    const claims = token ? verifyJwt(token, cfg.jwtSecret) : undefined;

    if (url.pathname !== "/ws/agent" && url.pathname !== "/ws/admin") {
      socket.destroy();
      return;
    }
    if (hub.draining) {
      socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
      socket.destroy();
      return;
    }
    const unauthorized = (): void => {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
    };
    // /ws/admin 仅接受 JWT；/ws/agent 在无 token 时接受设备密钥（?key=）或配对模式（?pair=1）
    const deviceKey = url.pathname === "/ws/agent" && token === ""
      ? url.searchParams.get("key") ?? ""
      : "";
    const pairing = url.pathname === "/ws/agent" && token === "" && deviceKey === ""
      && url.searchParams.get("pair") === "1";
    if (claims === undefined && deviceKey === "" && !pairing) {
      unauthorized();
      return;
    }

    void (async () => {
      let userID = "";
      let deviceKeyID: string | undefined;
      if (pairing) {
        // 无凭证配对连接：只允许 connector.pair，审批下发密钥后重连
      } else if (claims) {
        userID = claims.sub;
      } else {
        // 设备密钥认证：未知/禁用/属主禁用统一裸 401，防探测
        if (!hub.db) {
          unauthorized();
          return;
        }
        const key = await hub.db.getDeviceKeyByHash(hashDeviceKey(deviceKey)).catch(() => undefined);
        if (!key || key.disabled === 1) {
          unauthorized();
          return;
        }
        userID = key.owner_id;
        deviceKeyID = key.id;
      }

      // 禁用账号即时生效（JWT 未过期也拒绝新连接）；顺带缓存 admin 角色
      let isAdmin = false;
      if (!pairing && hub.db) {
        const u = await hub.db.getUserById(userID).catch(() => undefined);
        if (!u || u.disabled === 1) {
          unauthorized();
          return;
        }
        isAdmin = u.role === "admin";
      }
      if (deviceKeyID !== undefined && hub.db) {
        hub.db.touchDeviceKeyUsed(deviceKeyID).catch(() => {});
      }

      wss.handleUpgrade(req, socket, head, (ws) => {
      if (url.pathname === "/ws/agent") {
        const agent: AgentConn = {
          id: "",
          ownerID: userID,
          name: "",
          ws,
          capabilities: [],
          status: proto.AGENT_STATUS_ONLINE,
          lastHeartbeat: Date.now(),
          alive: true,
          deviceKeyID,
          ip: clientIp(req.headers, req.socket.remoteAddress, cfg.trustProxy),
          pairing,
        };
        const ticker = watchPong(ws, agent, () => {
          // pong = 连接活着：connector 模式一条 ws 托管多 agent，全部续命
          hub.livenessProbes.delete(ws);
          for (const a of hub.agents.values()) {
            if (a.ws === ws) a.lastHeartbeat = Date.now();
          }
        });
        ws.on("message", (data) => handleAgentMessage(hub, agent, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          hub.livenessProbes.delete(ws);
          // 一条连接可能托管多个 agent（connector 模式）：按 ws 全部注销
          const ids = [...hub.agents.values()].filter((a) => a.ws === ws).map((a) => a.id);
          for (const id of ids) hub.unregisterAgent(id);
          if (agent.connectorID) hub.unregisterConnector(agent.connectorID, ws);
          // 配对挂起连接断开：移出待接入列表
          for (const [cid, p] of hub.pendingPairs) {
            if (p.conn.ws === ws) hub.pendingPairs.delete(cid);
          }
        });
        ws.on("error", () => ws.close());
      } else {
        const user: UserConn = {
          ws, userID, lastHeartbeat: Date.now(), alive: true, isAdmin,
          ownOnly: url.searchParams.get("scope") === "own",
        };
        hub.registerUser(user);
        const ticker = watchPong(ws, user, () => {
          hub.livenessProbes.delete(ws);
          user.lastHeartbeat = Date.now();
        });
        ws.on("message", (data) => handleUserMessage(hub, user, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          hub.livenessProbes.delete(ws);
          hub.unregisterUser(ws);
        });
        ws.on("error", () => ws.close());
      }
      });
    })().catch(() => socket.destroy());
  });

  return { server, hub, wss, bus };
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

const JWT_SECRET_FILE = path.resolve("data/jwt-secret");

async function loadOrCreateJwtSecretFile(): Promise<{ secret: string; source: "loaded" | "generated" }> {
  await fsp.mkdir(path.dirname(JWT_SECRET_FILE), { recursive: true });
  try {
    const raw = await fsp.readFile(JWT_SECRET_FILE, "utf8");
    const secret = raw.trim();
    if (secret.length >= 32) return { secret, source: "loaded" };
    logger.warn("data/jwt-secret 内容过短或损坏，重新生成", { len: secret.length });
  } catch (e) {
    // 文件不存在时落入下方生成分支
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  const secret = crypto.randomBytes(32).toString("hex");
  await fsp.writeFile(JWT_SECRET_FILE, secret, { mode: 0o600 });
  return { secret, source: "generated" };
}

async function rotateJwtSecret(): Promise<void> {
  if (process.env.AGENT_MANAGE_JWT_SECRET && process.env.AGENT_MANAGE_JWT_SECRET !== "") {
    logger.error("已通过 AGENT_MANAGE_JWT_SECRET 环境变量提供 secret，请改环境变量来轮换；文件方式不生效");
    process.exit(1);
  }
  await fsp.mkdir(path.dirname(JWT_SECRET_FILE), { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);
  try {
    await fsp.copyFile(JWT_SECRET_FILE, `${JWT_SECRET_FILE}.${stamp}.bak`);
    logger.info("已备份旧 secret", { backup: `${JWT_SECRET_FILE}.${stamp}.bak` });
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  const fresh = crypto.randomBytes(32).toString("hex");
  await fsp.writeFile(JWT_SECRET_FILE, fresh, { mode: 0o600 });
  logger.info("已写入新 secret", { file: JWT_SECRET_FILE });
  logger.warn("重启 gateway 后新 secret 生效；届时所有现有 token 失效，用户需重新登录、Agent 需重连");
}

if (isMain) {
  if (process.argv.includes("--rotate-secret") || process.argv.includes("-rotate-secret")) {
    void rotateJwtSecret().then(() => process.exit(0));
  } else {
    void main();
  }
}

async function main(): Promise<void> {
  const cfg = loadGatewayConfig();
  setLogLevel(cfg.logLevel);
  if (cfg.jwtSecret === "") {
    if (process.env.AGENT_MANAGE_JWT_SECRET && process.env.AGENT_MANAGE_JWT_SECRET !== "") {
      // 理论上 loadGatewayConfig 已注入；保险一行
      cfg.jwtSecret = process.env.AGENT_MANAGE_JWT_SECRET;
    } else {
      const r = await loadOrCreateJwtSecretFile();
      cfg.jwtSecret = r.secret;
      if (r.source === "loaded") {
        logger.info("jwt secret 已从 data/jwt-secret 加载（重启后 token 仍然有效）");
      } else {
        logger.warn("已生成新 jwt secret 并保存到 data/jwt-secret（后续重启会复用此 secret）");
      }
    }
  }
  const db = new Db(cfg.databaseURL);
  await db.init();
  logger.info("database connected", { url: cfg.databaseURL.replace(/:\/\/[^@]*@/, "://***@") });
  const admin = await db.getUserByName("admin");
  if (!admin) {
    await db.createUser({
      id: crypto.randomUUID(),
      name: "admin",
      password_hash: hashPassword(cfg.adminPassword),
      role: "admin",
    });
    logger.warn("已创建初始 admin 账号，请尽快修改默认密码（AGENT_MANAGE_ADMIN_PASSWORD）");
  }
  const staticFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
  let attachments: AttachmentStore | undefined;
  if (cfg.s3Endpoint !== "") {
    attachments = await createS3AttachmentStore({
      endpoint: cfg.s3Endpoint,
      region: cfg.s3Region,
      bucket: cfg.s3Bucket,
      accessKey: cfg.s3AccessKey,
      secretKey: cfg.s3SecretKey,
      publicURLBase: cfg.s3PublicURL !== "" ? cfg.s3PublicURL : undefined,
    });
    if (attachments) logger.info("attachment store ready (s3)", { endpoint: cfg.s3Endpoint, bucket: cfg.s3Bucket });
  } else if (cfg.attachDir !== "") {
    attachments = await createLocalAttachmentStore(path.resolve(cfg.attachDir));
    if (attachments) logger.info("attachment store ready (local)", { dir: path.resolve(cfg.attachDir) });
  }
  const { server, hub } = await createGatewayServer(cfg, staticFile, db, attachments);
  // 保留策略：每天清理 updated_at 早于 retentionDays 的会话（级联消息与附件）
  if (cfg.retentionDays > 0) {
    const purge = async () => {
      const cutoff = Date.now() - cfg.retentionDays * 86400_000;
      const old = await db.listOldSessions(cutoff);
      for (const s of old) {
        await deleteSessionWithAttachments(hub, s.owner_id, s.id);
      }
      if (old.length) logger.info("retention purge", { sessions: old.length, retention_days: cfg.retentionDays });
    };
    void purge().catch((e) => logger.error("retention purge failed", { error: String(e) }));
    setInterval(() => {
      void purge().catch((e) => logger.error("retention purge failed", { error: String(e) }));
    }, 86400_000).unref();
    logger.info("retention policy enabled", { days: cfg.retentionDays });
  }
  const { host, port } = parseListenAddr(cfg.addr);
  server.listen(port, host, () => {
    logger.info("gateway listening", { addr: cfg.addr });
  });

  // 优雅关闭：healthz 立即 503 供 LB 摘流，断 WS 让 client 重连到其他实例，
  // 留短宽限给消息落库与 close 握手，随后停止接受新连接并关 Redis/MySQL 退出
  let stopping = false;
  const shutdown = (signal: string) => {
    if (stopping) return;
    stopping = true;
    logger.info("shutting down", { signal });
    hub.shutdown();
    const force = setTimeout(() => process.exit(1), 10_000);
    force.unref();
    setTimeout(() => {
      void (async () => {
        server.close();
        server.closeAllConnections();
        await hub.bus?.stop().catch((e) => logger.error("bus stop failed", { error: String(e) }));
        await db.close().catch((e) => logger.error("db close failed", { error: String(e) }));
        logger.info("shutdown complete");
        process.exit(0);
      })();
    }, 1500);
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}
