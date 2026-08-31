// Hub：连接注册表、任务跟踪/超时、消息落库缓冲、编排回投与 collect 收割、
// 指标与广播。附带 sendError / sendMsg / withDb 三个 RPC 基础设施。
import crypto from "node:crypto";
import { WebSocket } from "ws";
import * as proto from "../protocol.ts";
import { logger } from "../util.ts";
import { Db, type DbAgentBrand } from "../db.ts";
import { Bus, type RegisteredAgent } from "../bus.ts";
import type { AttachmentStore } from "../storage.ts";
import { Metrics } from "../metrics.ts";
import { RateLimiter } from "../ratelimit.ts";
import {
  type AgentConn, type PendingPair, type UserConn, type TaskState, type InvocationRecord,
  type PendingEntry, type TaskBuffer, type BufferedInteractionChunk,
  isInteractionChunk, matchesInteractionChunk, finalOnlyChunks, textOfChunk,
  MAX_TASK_BUFFER_BYTES, SEND_BUFFER_SOFT, SEND_BUFFER_HARD,
} from "./types.ts";

export class Hub {
  agents = new Map<string, AgentConn>();
  users = new Map<WebSocket, UserConn>();
  connectors = new Map<string, AgentConn>(); // connector 模式 client 连接（id = connector_id）
  pendingPairs = new Map<string, PendingPair>(); // 待审批的配对连接（id = connector_id）
  pendingRequests = new Map<string, PendingEntry>();
  tasks = new Map<string, TaskState>();
  taskBuffers = new Map<string, TaskBuffer>();
  // 模板引擎（group.run）等待任务终结的回调队列；settleWaiters 在终态 funnel 唤醒
  taskWaiters = new Map<string, Array<(r: { error?: string; text: string }) => void>>();
  // 编排续聊线程：(groupID, invoker, target, thread_id) → 目标 agent 所见的稳定子会话 id。
  // 内存态：网关重启后同线程再调用会拿到新 id（目标 agent 侧上下文重新开始）
  invocationThreads = new Map<string, string>();
  // 群上下文注入用的 agent 档案缓存（ownerID → id→{name,capabilities}），60s TTL + 注册/注销失效
  private agentProfileCache = new Map<string, { at: number; profiles: Map<string, { name: string; capabilities: proto.Capability[] }> }>();
  // 在线 agent id 集缓存（本地 ∪ 注册表），5s TTL
  private onlineCache: { at: number; ids: Set<string> } | null = null;
  db?: Db;
  bus?: Bus;
  attachments?: AttachmentStore;
  productsDir?: string; // 产品分发目录；product.push 用它校验 brand/version 真实存在
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
    this.metrics.counter("ywm_ws_send_dropped_total", "Messages dropped by WS send backpressure");
    this.metrics.counter("ywm_task_duration_seconds_sum", "Total task duration seconds");
    this.metrics.counter("ywm_task_duration_seconds_count", "Duration sample count");
    this.metrics.counter("ywm_messages_persisted_total", "Messages written to MySQL");
    this.metrics.counter("ywm_attachments_uploaded_total", "Attachment uploads");
    this.metrics.counter("ywm_attachment_bytes_total", "Attachment bytes uploaded");
    this.metrics.counter("ywm_subtasks_created_total", "Orchestration subtasks dispatched");
    this.metrics.counter("ywm_subtasks_completed_total", "Subtasks finished with done");
    this.metrics.counter("ywm_subtasks_failed_total", "Subtasks finished with error");
    this.metrics.counter("ywm_subtasks_timeout_total", "Subtasks killed by timeout");
    this.metrics.counter("ywm_subtasks_cancelled_total", "Subtasks cancelled");
    this.metrics.counter("ywm_subtask_duration_seconds_sum", "Subtask duration seconds total");
    this.metrics.counter("ywm_subtask_duration_seconds_count", "Subtask duration sample count");
  }

  // ---- 编排运行记录（durable run tree） ----

  // dispatch 时写 running 行（fire-and-forget，与消息持久化同风格）
  recordRunStart(taskID: string, ts: TaskState): void {
    if (!this.db || !ts.parentTaskID) return;
    this.metrics.inc("ywm_subtasks_created_total");
    const db = this.db;
    db.createRun({
      id: taskID,
      owner_id: ts.ownerID,
      group_id: ts.groupID ?? "",
      parent_task_id: ts.parentTaskID,
      invoker_agent_id: ts.invokerAgentID ?? "",
      target_agent_id: ts.agentID,
      invocation_id: ts.invocationID ?? null,
      session_id: ts.sessionID,
      instance_id: this.bus?.instanceID ?? "local",
      status: "running",
      created_at: ts.createdAt,
    }).catch((e) => logger.error("run create failed", { error: String(e) }));
  }

  // 子任务终态唯一 funnel：指标 + orchestration_runs 收口（running → 终态，迟到信号不覆盖）。
  // 普通任务（无 parentTaskID）直接跳过
  finishRun(taskID: string, status: "completed" | "failed" | "timeout" | "cancelled", error?: string): void {
    const ts = this.tasks.get(taskID);
    if (!ts?.parentTaskID) return;
    this.metrics.inc(`ywm_subtasks_${status}_total`);
    this.metrics.inc("ywm_subtask_duration_seconds_sum", (Date.now() - ts.createdAt) / 1000);
    this.metrics.inc("ywm_subtask_duration_seconds_count");
    if (!this.db) return;
    this.db.finishRun(taskID, status, error ?? null, Date.now())
      .catch((e) => logger.error("run finish failed", { error: String(e), task_id: taskID }));
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
    this.invalidateAgentNames(a.ownerID);
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
    if (ownerID !== "") this.invalidateAgentNames(ownerID);
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

  // 群上下文注入用的 agent 档案（ownerID → id→{name,capabilities}）：
  // 60s TTL + 注册/注销时失效，避免每条群消息全量 listAgentsPaged(1000)
  async agentProfilesOf(ownerID: string): Promise<Map<string, { name: string; capabilities: proto.Capability[] }>> {
    const hit = this.agentProfileCache.get(ownerID);
    if (hit && Date.now() - hit.at < 60_000) return hit.profiles;
    const profiles = new Map<string, { name: string; capabilities: proto.Capability[] }>();
    if (this.db) {
      try {
        const page = await this.db.listAgentsPaged({ ownerID, limit: 1000, offset: 0 });
        for (const a of page.agents) {
          let caps: proto.Capability[] = [];
          try { caps = a.capabilities ? JSON.parse(a.capabilities) as proto.Capability[] : []; } catch { /* 坏数据忽略 */ }
          if (!Array.isArray(caps)) caps = [];
          profiles.set(a.id, { name: a.name, capabilities: caps });
        }
      } catch { /* 查询失败退空表，调用方回退 agent id */ }
    }
    this.agentProfileCache.set(ownerID, { at: Date.now(), profiles });
    return profiles;
  }

  invalidateAgentNames(ownerID: string): void {
    this.agentProfileCache.delete(ownerID);
  }

  // 在线 agent id 集（本地 ∪ 注册表），5s 缓存：metadata.group.members[].online 的数据源
  async onlineAgentIDs(): Promise<Set<string>> {
    if (this.onlineCache && Date.now() - this.onlineCache.at < 5_000) return this.onlineCache.ids;
    const ids = new Set(this.agents.keys());
    if (this.bus) {
      try {
        for (const a of await this.bus.listAgents()) ids.add(a.id);
      } catch { /* 注册表不可用时仅本地 */ }
    }
    this.onlineCache = { at: Date.now(), ids };
    return ids;
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
    logger.warn("agent not found", { agent_id: agentID, method: msg.method, task: (msg.params as { task_id?: string } | null)?.task_id });
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
    this.finishRun(p.taskID, "failed", reason);
    this.observeTaskEnd(p.taskID, "failed");
    this.settleWaiters(p.taskID, reason);
    this.untrackTask(p.taskID);
  }

  trackTask(taskID: string, agentID: string, ownerID: string, sessionID = "", extra?: Partial<TaskState> & { timeoutMs?: number }): void {
    const { timeoutMs, ...state } = extra ?? {};
    const timer = setTimeout(() => this.taskTimeoutCallback(taskID), timeoutMs ?? this.taskTimeoutMs);
    timer.unref();
    this.tasks.set(taskID, { agentID, ownerID, sessionID, timer, createdAt: Date.now(), depth: 0, ...state });
    this.metrics.inc("ywm_tasks_created_total");
  }

  untrackTask(taskID: string): void {
    const ts = this.tasks.get(taskID);
    if (ts) {
      clearTimeout(ts.timer);
      this.tasks.delete(taskID);
      // 父任务终结：其名下 invocation 记录一并消亡（子任务的记录引用同Map，无需单独清理）
      if (ts.invocations) ts.invocations.clear();
    }
    // 兜底：任务以非标准路径移除时唤醒还在等的模板引擎
    if (this.taskWaiters.has(taskID)) this.settleWaiters(taskID, "task ended");
  }

  // ---- 声明式运行模板（group.run）的完成等待 ----

  // 等待任务终结（done/error/timeout/拒绝任一）；任务不存在立即回错误
  waitTaskDone(taskID: string): Promise<{ error?: string; text: string }> {
    return new Promise((resolve) => {
      if (!this.tasks.has(taskID)) {
        resolve({ error: "task not found", text: "" });
        return;
      }
      const list = this.taskWaiters.get(taskID) ?? [];
      list.push(resolve);
      this.taskWaiters.set(taskID, list);
    });
  }

  // 终态 funnel 的一站：提取 final_only 终态文本并唤醒等待者。
  // 须在 flushTaskBuffer 之前调用（缓冲还在才有全文）
  settleWaiters(taskID: string, error?: string): void {
    const waiters = this.taskWaiters.get(taskID);
    if (!waiters) return;
    this.taskWaiters.delete(taskID);
    let chunks: proto.LocalAgentChunk[] = [...(this.taskBuffers.get(taskID)?.chunks ?? [])];
    if (error) chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(error) });
    const text = finalOnlyChunks(chunks).map(textOfChunk).join("");
    for (const w of waiters) w({ error, text });
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

  // 管理者编排：子任务终结（done/error/timeout）时把结果回投给管理者 agent。
  // context_policy=final_only 只回终态文本+产出（默认，管理者上下文不被过程噪音撑爆）；
  // full 回全量 chunks（旧行为）。终态结果留存到父任务的 invocation 记录，幂等重发用。
  notifySubtaskResult(taskID: string, ts: TaskState, errorText?: string): void {
    if (!ts.parentTaskID || !ts.invokerAgentID) return;
    const buf = this.taskBuffers.get(taskID);
    let chunks: proto.LocalAgentChunk[] = [...(buf?.chunks ?? [])];
    if (buf?.truncated) chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent("（输出过长，中间内容已截断）") });
    if (errorText) chunks.push({ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(errorText) });
    const policy = ts.contextPolicy ?? "final_only";
    if (policy === "final_only") chunks = finalOnlyChunks(chunks);
    const params: proto.AgentTaskResultParams = {
      agent_id: ts.invokerAgentID, // connector 多实例托管时 client 按此路由到管理者实例
      task_id: taskID,
      parent_task_id: ts.parentTaskID,
      group_id: ts.groupID ?? "",
      target_agent_id: ts.agentID,
      status: errorText ? "failed" : "completed",
      chunks,
      error: errorText,
      context_policy: policy,
      invocation_id: ts.invocationID,
    };
    // 留存到父任务的 invocation 记录（有的话），duplicate invoke 已结束时据此重发
    const parent = this.tasks.get(ts.parentTaskID);
    const record = ts.invocationID ? parent?.invocations?.get(ts.invocationID) : undefined;
    if (record) {
      const child = record.children.find((c) => c.taskID === taskID);
      if (child) {
        child.done = true;
        child.ok = !errorText;
        child.lastResult = params;
      }
    }
    this.forwardToAgent(ts.invokerAgentID, proto.newNotification(proto.METHOD_AGENT_TASK_RESULT, params));
    // 真实结果先到，收割的合成结果在后（管理者先看到赢家，再看到被取消的兄弟）
    if (record) this.harvestCollect(record);
  }

  // collect 收割：first/quorum 条件满足时取消其余运行中的兄弟任务（竞速/多数派）。
  // 被收割的子任务立即合成 failed 结果回投管理者并就地清理，不等 agent 补终态
  private harvestCollect(record: InvocationRecord): void {
    if (record.collected || record.collect === "all") return;
    const need = record.collect === "first" ? 1 : record.collect.quorum;
    if (record.children.filter((c) => c.ok).length < need) return;
    record.collected = true;
    for (const c of record.children) {
      if (c.done) continue;
      const cts = this.tasks.get(c.taskID);
      if (!cts) { c.done = true; c.ok = false; continue; }
      c.done = true;
      c.ok = false;
      const reason = "cancelled: collect condition met";
      c.lastResult = {
        agent_id: cts.invokerAgentID ?? "",
        task_id: c.taskID,
        parent_task_id: cts.parentTaskID ?? "",
        group_id: cts.groupID ?? "",
        target_agent_id: cts.agentID,
        status: "failed",
        error: reason,
        context_policy: cts.contextPolicy ?? "final_only",
        invocation_id: cts.invocationID,
      };
      this.finishRun(c.taskID, "cancelled", reason);
      this.observeTaskEnd(c.taskID, "failed");
      this.untrackTask(c.taskID);
      this.flushTaskBuffer(c.taskID, reason);
      this.forwardToAgent(cts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
        task_id: c.taskID,
        session_id: cts.threadSessionID || cts.sessionID || undefined,
      } satisfies proto.AgentCancelParams));
      this.forwardToAgent(cts.invokerAgentID!, proto.newNotification(proto.METHOD_AGENT_TASK_RESULT, c.lastResult));
    }
  }

  // C6 父子任务取消：父任务取消/超时时，按 parentTaskID 链 BFS 级联取消全部未完成后代
  // （多级编排下含孙任务）。tasks 只存未完成任务（done 即 untrack），无需再筛状态；
  // 任务清理仍由各子任务 done 进度驱动。
  cascadeCancelSubtasks(parentTaskID: string, allow?: (ts: TaskState) => boolean): void {
    const queue = [parentTaskID];
    const seen = new Set<string>([parentTaskID]);
    while (queue.length > 0) {
      const cur = queue.shift()!;
      for (const [tid, ts] of this.tasks) {
        if (ts.parentTaskID !== cur || seen.has(tid)) continue;
        if (allow && !allow(ts)) continue;
        seen.add(tid);
        queue.push(tid);
        logger.info("cascade cancel subtask", { task_id: tid, parent_task_id: cur });
        this.finishRun(tid, "cancelled", "parent cancelled");
        this.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
          task_id: tid,
          session_id: ts.threadSessionID || ts.sessionID || undefined,
        } satisfies proto.AgentCancelParams));
      }
    }
  }

  private taskTimeoutCallback(taskID: string): void {
    const ts = this.tasks.get(taskID);
    if (!ts) return;
    this.observeTaskEnd(taskID, "timeout");
    this.finishRun(taskID, "timeout", "任务超时");
    this.settleWaiters(taskID, "任务超时");
    this.tasks.delete(taskID);
    logger.warn("task timeout", { task_id: taskID, agent_id: ts.agentID });
    this.notifySubtaskResult(taskID, ts, "任务超时");
    this.flushTaskBuffer(taskID, "任务超时");
    // 通知 agent 中止任务，避免网关侧超时后 agent 还在空跑
    this.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
      task_id: taskID,
      session_id: ts.threadSessionID || ts.sessionID || undefined,
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

  trySend(ws: WebSocket, msg: proto.Message): boolean {
    if (ws.readyState !== WebSocket.OPEN) return false;
    // 发送背压：慢消费者（合盖/断网）的发送缓冲会无限堆积直至 OOM。
    // 软水位丢弃本条消息（通知类可丢，客户端重连后靠对账恢复）；
    // 硬水位说明对端已停摆，terminate 立即释放内存（close 还会往同一缓冲排队）。
    if (ws.bufferedAmount >= SEND_BUFFER_SOFT) {
      this.metrics.inc("ywm_ws_send_dropped_total");
      if (ws.bufferedAmount >= SEND_BUFFER_HARD) {
        logger.warn("send buffer overflow, terminating stalled consumer", {
          buffered: ws.bufferedAmount,
        });
        ws.terminate();
      }
      return false;
    }
    ws.send(JSON.stringify(msg));
    return true;
  }
}

export function sendError(ws: WebSocket, id: string | undefined, code: number, message: string, data?: unknown): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount >= SEND_BUFFER_SOFT) return; // 背压：丢弃响应，调用方超时兜底
  ws.send(JSON.stringify(proto.newErrorResponse(id ?? "", code, message, data)));
}

export function sendMsg(ws: WebSocket, msg: proto.Message): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount >= SEND_BUFFER_SOFT) return; // 背压：丢弃响应，调用方超时兜底
  ws.send(JSON.stringify(msg));
}

// db 未配置时返回错误；否则执行异步存储操作并把异常转为 JSON-RPC 错误。
export function withDb(hub: Hub, user: UserConn, msg: proto.Message, fn: (db: Db) => Promise<void>): void {
  if (!hub.db) {
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  fn(hub.db).catch((e) => {
    logger.error("storage op failed", { method: msg.method, error: String(e) });
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage error");
  });
}

