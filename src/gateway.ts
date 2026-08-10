import http from "node:http";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";
import * as proto from "./protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, setLogLevel, logger, parseListenAddr } from "./util.ts";
import { Db, type DbUser } from "./db.ts";
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
}

interface UserConn {
  ws: WebSocket;
  userID: string;
  lastHeartbeat: number;
  alive: boolean;
  isAdmin: boolean; // WS 升级时缓存的角色；角色变更通过 kickUser 强制重连刷新
}

interface TaskState {
  agentID: string;
  ownerID: string;
  sessionID: string;
  timer: NodeJS.Timeout;
  createdAt: number;
}

interface TaskBuffer {
  ownerID: string;
  agentID: string;
  sessionID: string;
  chunks: proto.LocalAgentChunk[];
}

export class Hub {
  agents = new Map<string, AgentConn>();
  users = new Map<WebSocket, UserConn>();
  pendingRequests = new Map<string, UserConn>();
  tasks = new Map<string, TaskState>();
  taskBuffers = new Map<string, TaskBuffer>();
  db?: Db;
  bus?: Bus;
  attachments?: AttachmentStore;
  metrics = new Metrics();
  taskLimiter = new RateLimiter(30, 60_000); // 每用户每分钟 30 个任务
  deviceKeyLimiter = new RateLimiter(10, 60_000); // 每用户每分钟 10 次密钥创建
  draining = false;

  private agentTimeoutMs: number;
  private userTimeoutMs: number;
  private taskTimeoutMs: number;
  private checker: NodeJS.Timeout;

  constructor(agentTimeoutMs: number, userTimeoutMs: number, taskTimeoutMs: number) {
    this.agentTimeoutMs = agentTimeoutMs;
    this.userTimeoutMs = userTimeoutMs;
    this.taskTimeoutMs = taskTimeoutMs;
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
    for (const ts of this.tasks.values()) clearTimeout(ts.timer);
    for (const taskID of [...this.taskBuffers.keys()]) this.flushTaskBuffer(taskID);
    for (const [reqID, u] of this.pendingRequests) {
      try {
        u.ws.send(JSON.stringify(proto.newErrorResponse(reqID, proto.ERR_INTERNAL_ERROR, "server shutting down")));
      } catch { /* 连接可能已断开 */ }
    }
    this.pendingRequests.clear();
    for (const agent of this.agents.values()) agent.ws.close(1001, "server shutting down");
    for (const ws of this.users.keys()) ws.close(1001, "server shutting down");
  }

  private heartbeatCheck(): void {
    for (const [id, agent] of this.agents) {
      if (Date.now() - agent.lastHeartbeat > this.agentTimeoutMs) {
        logger.warn("agent heartbeat timeout", { agent_id: id });
        this.unregisterAgent(id);
        agent.ws.close();
      }
    }
    for (const [ws, user] of this.users) {
      if (Date.now() - user.lastHeartbeat > this.userTimeoutMs) {
        logger.warn("user heartbeat timeout", { user_id: user.userID });
        this.users.delete(ws);
        for (const [reqID, u] of this.pendingRequests) {
          if (u.ws === ws) this.pendingRequests.delete(reqID);
        }
        ws.close();
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
    };
  }

  // 心跳/状态变化时刷新注册表 TTL 与内容（单机模式为 no-op）
  refreshAgentRegistry(a: AgentConn): void {
    if (!this.bus || a.id === "") return;
    this.bus.refreshAgent(this.registeredAgentOf(a))
      .catch((e) => logger.error("registry refresh failed", { error: String(e) }));
  }

  registerAgent(a: AgentConn): void {
    a.lastHeartbeat = Date.now();
    this.agents.set(a.id, a);
    if (this.bus) {
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
    for (const [reqID, u] of this.pendingRequests) {
      if (u.ws === ws) this.pendingRequests.delete(reqID);
    }
  }

  // 禁用/改密后踢掉该用户的所有页面连接（agent 连接不受影响，下次心跳超时自然清理）
  kickUser(userID: string): void {
    for (const u of this.users.values()) {
      if (u.userID === userID) u.ws.close(4001, "account disabled or password changed");
    }
    for (const a of this.agents.values()) {
      if (a.ownerID === userID) a.ws.close(4001, "account disabled or password changed");
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

  async agentList(): Promise<proto.AgentInfo[]> {
    const byID = new Map<string, proto.AgentInfo>();
    if (this.bus) {
      try {
        for (const a of await this.bus.listAgents()) {
          byID.set(a.id, {
            id: a.id,
            owner_id: a.owner_id,
            name: a.name,
            status: a.status || proto.AGENT_STATUS_ONLINE,
            capabilities: a.capabilities,
            platform: a.platform,
            last_heartbeat: new Date(a.last_heartbeat).toISOString(),
          });
        }
      } catch (e) {
        logger.error("registry list failed", { error: String(e) });
      }
    }
    // 本地连接的信息最新，覆盖注册表中的同 id 条目
    for (const [id, a] of this.agents) {
      byID.set(id, {
        id,
        owner_id: a.ownerID,
        name: a.name,
        status: a.status || proto.AGENT_STATUS_ONLINE,
        capabilities: a.capabilities,
        platform: a.platform,
        last_heartbeat: new Date(a.lastHeartbeat).toISOString(),
      });
    }
    return [...byID.values()];
  }

  private filterAgentsForUser(agents: proto.AgentInfo[], user: UserConn): proto.AgentInfo[] {
    if (user.isAdmin) return agents;
    return agents.filter((a) => a.owner_id === user.userID);
  }

  broadcastAgentList(): void {
    void this.agentList().then((agents) => {
      for (const user of this.users.values()) {
        const filtered = this.filterAgentsForUser(agents, user);
        const msg = proto.newNotification(proto.METHOD_ADMIN_AGENT_LIST, { agents: filtered });
        this.trySend(user.ws, msg);
      }
    }).catch((e) => logger.error("broadcast agent list failed", { error: String(e) }));
  }

  private sendAgentList(u: UserConn): void {
    void this.agentList().then((agents) => {
      const filtered = this.filterAgentsForUser(agents, u);
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
    // admin 需要看到全量事件（跨实例时其他实例的 admin 靠 agent.list 刷新兜底）
    for (const u of this.users.values()) {
      if (u.isAdmin && u.userID !== ownerID) this.trySend(u.ws, msg);
    }
  }

  forwardToAgent(agentID: string, msg: proto.Message): void {
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
    const user = this.pendingRequests.get(id);
    if (!user) return false;
    this.pendingRequests.delete(id);
    this.trySend(user.ws, msg);
    return true;
  }

  trackPendingRequest(id: string, user: UserConn): void {
    this.pendingRequests.set(id, user);
  }

  trackTask(taskID: string, agentID: string, ownerID: string, sessionID = ""): void {
    const timer = setTimeout(() => this.taskTimeoutCallback(taskID), this.taskTimeoutMs);
    timer.unref();
    this.tasks.set(taskID, { agentID, ownerID, sessionID, timer, createdAt: Date.now() });
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

  persistUserMessage(params: proto.TaskCreateParams, sessionID: string, ownerID: string): void {
    if (!this.db) return;
    const db = this.db;
    const now = Date.now();
    const run = async () => {
      const existing = await db.getSession(ownerID, sessionID);
      if (!existing) {
        const title = params.content.trim().replace(/\s+/g, " ").slice(0, 20) || "新会话";
        await db.createSession({ id: sessionID, owner_id: ownerID, agent_id: params.agent_id, title });
      }
      await db.appendMessage({
        id: crypto.randomUUID(),
        session_id: sessionID,
        owner_id: ownerID,
        agent_id: params.agent_id,
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
      buf = { ownerID, agentID, sessionID, chunks: [] };
      this.taskBuffers.set(taskID, buf);
    }
    buf.chunks.push(chunk);
  }

  flushTaskBuffer(taskID: string, errorText?: string): void {
    const buf = this.taskBuffers.get(taskID);
    if (!buf || !this.db) {
      this.taskBuffers.delete(taskID);
      return;
    }
    this.taskBuffers.delete(taskID);
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

  private taskTimeoutCallback(taskID: string): void {
    const ts = this.tasks.get(taskID);
    if (!ts) return;
    this.observeTaskEnd(taskID, "timeout");
    this.tasks.delete(taskID);
    logger.warn("task timeout", { task_id: taskID, agent_id: ts.agentID });
    this.flushTaskBuffer(taskID, "任务超时");
    const notif = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, {
      task_id: taskID,
      agent_id: ts.agentID,
      done: true,
      error: "timeout",
    } satisfies proto.AdminProgressParams);
    this.forwardToUsers(ts.ownerID, notif);
  }

  private trySend(ws: WebSocket, msg: proto.Message): void {
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
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    sessions: sessions.map((s) => ({
      id: s.id,
      agent_id: s.agent_id,
      title: s.title,
      created_at: s.created_at,
      updated_at: s.updated_at,
      message_count: Number(s.message_count ?? 0),
    })),
  } satisfies proto.SessionListResult));
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
  const s = await db.createSession({
    id: params.id || crypto.randomUUID(),
    owner_id: user.userID,
    agent_id: params.agent_id,
    title: params.title?.trim() || "新会话",
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", sessionInfoOf(s)));
}

function sessionInfoOf(s: { id: string; agent_id: string; title: string; created_at: number; updated_at: number; message_count?: number }): proto.SessionInfo {
  return {
    id: s.id,
    agent_id: s.agent_id,
    title: s.title,
    created_at: s.created_at,
    updated_at: s.updated_at,
    message_count: Number(s.message_count ?? 0),
  };
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

// ---- 用户管理 ----

function userInfoOf(u: DbUser): proto.UserInfo {
  return { id: u.id, name: u.name, role: u.role, disabled: u.disabled === 1, created_at: u.created_at, last_login_at: u.last_login_at };
}

async function requireAdmin(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<boolean> {
  const me = await db.getUserById(user.userID);
  if (me?.role !== "admin") {
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
  const u: DbUser = {
    id: crypto.randomUUID(),
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
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AdminAgentListParams>(msg);
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const offset = Math.max(params.offset ?? 0, 0);
  const { agents, total } = await db.listAgentsPaged({
    ownerID: params.owner_id || undefined,
    status: params.status || undefined,
    query: params.query || undefined,
    limit,
    offset,
  });
  // 实时状态优先：内存/注册表覆盖 DB 行的展示状态，DB 提供离线/历史记录
  const liveByID = new Map<string, proto.AgentInfo>();
  for (const a of await hub.agentList()) liveByID.set(a.id, a);
  const rows: proto.AdminAgentInfo[] = agents.map((row) => {
    const live = liveByID.get(row.id);
    let caps: proto.Capability[] = [];
    let plat: proto.PlatformInfo | undefined;
    try { caps = row.capabilities ? JSON.parse(row.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
    try { plat = row.platform ? JSON.parse(row.platform) as proto.PlatformInfo : undefined; } catch { /* 忽略坏数据 */ }
    return {
      id: row.id,
      owner_id: live?.owner_id ?? row.owner_id,
      name: live?.name ?? row.name,
      status: live?.status ?? row.status,
      capabilities: live?.capabilities ?? caps,
      platform: live?.platform ?? plat,
      last_heartbeat: live?.last_heartbeat ?? new Date(row.last_seen).toISOString(),
      first_seen: row.first_seen,
      last_seen: row.last_seen,
      online: live !== undefined,
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
    hub.refreshAgentRegistry(local);
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
  // 踢掉使用该密钥的在线 agent 连接
  for (const a of hub.agents.values()) {
    if (a.deviceKeyID === params.id) a.ws.close(4001, "device key revoked");
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
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

  switch (msg.method) {
    case proto.METHOD_REGISTER: {
      const params = proto.decodeParams<proto.RegisterParams>(msg);
      agent.id = params.agent_id;
      agent.name = params.name !== "" && params.name !== undefined ? params.name : params.agent_id;
      // Owner comes from token; register params owner is ignored for security.
      agent.capabilities = params.capabilities ?? [];
      agent.platform = params.platform;
      hub.registerAgent(agent);
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", {
        status: "ok",
        server_time: proto.rfc3339Now(),
      } satisfies proto.RegisterResult));
      break;
    }

    case proto.METHOD_CAPABILITIES_UPDATED: {
      const params = proto.decodeParams<proto.CapabilitiesUpdatedParams>(msg);
      const a = hub.agents.get(agent.id);
      if (a) a.capabilities = params.capabilities;
      hub.broadcastAgentList();
      break;
    }

    case proto.METHOD_HEARTBEAT: {
      agent.lastHeartbeat = Date.now();
      hub.refreshAgentRegistry(agent);
      hub.touchAgentThrottled(agent);
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_STATUS: {
      agent.lastHeartbeat = Date.now();
      const params = proto.decodeParams<proto.StatusParams>(msg);
      if (params.status && params.status !== agent.status) {
        agent.status = params.status;
        hub.refreshAgentRegistry(agent);
        hub.touchAgentThrottled(agent, true);
        if (agent.id !== "") hub.broadcastAgentList();
      }
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_PROGRESS: {
      const params = proto.decodeParams<proto.ProgressParams>(msg);
      const value = params.value;
      if (!value) break;
      if (!value.agent_id) value.agent_id = agent.id;
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
      hub.forwardToUsers(agent.ownerID, notif);
      // 跨属主任务（admin 操作他人 agent）：进度同时发给任务发起者
      const ts = hub.tasks.get(value.task_id);
      if (ts && ts.ownerID !== agent.ownerID) hub.forwardToUsers(ts.ownerID, notif);
      {
        const sessionID = value.session_id ?? ts?.sessionID ?? "";
        if (sessionID !== "") {
          hub.bufferProgressChunk(value.task_id, agent.ownerID, agent.id, sessionID,
            progress as unknown as proto.LocalAgentChunk);
        }
      }
      if (value.done || (value.error !== undefined && value.error !== "")) {
        hub.observeTaskEnd(value.task_id, value.error !== undefined && value.error !== "" ? "failed" : "completed");
        hub.untrackTask(value.task_id);
        hub.flushTaskBuffer(value.task_id, value.error);
      }
      break;
    }

    default: {
      if (msg.id) {
        hub.forwardToPendingUser(msg.id, msg);
      }
    }
  }
}

async function handleTaskCreate(hub: Hub, user: UserConn, msg: proto.Message): Promise<void> {
  const params = proto.decodeParams<proto.TaskCreateParams>(msg);
  const agent = await hub.resolveAgent(params.agent_id);
  if (!agent) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not found");
    return;
  }
  if (!hub.canManage(user, agent)) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to manage this agent");
    return;
  }
  if (!hub.taskLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks, please slow down");
    return;
  }
  if (msg.id) hub.trackPendingRequest(msg.id, user);
  const sessionID = params.session_id || `${params.task_id}-session`;
  hub.trackTask(params.task_id, params.agent_id, user.userID, sessionID);
  hub.persistUserMessage(params, sessionID, user.userID);

  hub.forwardToAgent(params.agent_id, proto.newRequest(msg.id ?? "", proto.METHOD_AGENT_CHAT, {
    task_id: params.task_id,
    session_id: sessionID,
    context_id: params.context_id,
    type: params.type,
    content: params.content,
    metadata: params.metadata,
  } satisfies proto.AgentChatParams));
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
      void handleTaskForward(hub, user, msg, proto.METHOD_AGENT_CANCEL, proto.decodeParams<proto.TaskCancelParams>(msg)).catch((e) => {
        logger.error("task.cancel failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_TASK_RESPOND: {
      const params = proto.decodeParams<proto.TaskRespondParams>(msg);
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

    case proto.METHOD_SESSION_DELETE:
      withDb(hub, user, msg, (db) => handleSessionDelete(hub, user, msg, db));
      break;

    case proto.METHOD_MESSAGE_LIST:
      withDb(hub, user, msg, (db) => handleMessageList(hub, user, msg, db));
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

    default:
      sendError(user.ws, msg.id, proto.ERR_METHOD_NOT_FOUND, `method not found: ${msg.method}`);
  }
}

// Watches a connection with periodic pings; terminates if no pong arrives
// between two pings (mirrors the Go read-deadline behavior).
function watchPong(ws: WebSocket, conn: { alive: boolean }, onPong?: () => void): NodeJS.Timeout {
  ws.on("pong", () => {
    conn.alive = true;
    onPong?.();
  });
  const ticker = setInterval(() => {
    if (!conn.alive) {
      ws.terminate();
      return;
    }
    conn.alive = false;
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

export function loadGatewayConfig(argv?: string[]): GatewayConfig {
  const specs = [
    { name: "addr", type: "string" as const, default: envString("AGENT_MANAGE_ADDR", ":8080") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "agent-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_AGENT_TIMEOUT", 90_000)) },
    { name: "user-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_USER_TIMEOUT", 120_000)) },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 1_800_000)) },
    { name: "database-url", type: "string" as const, default: envString("AGENT_MANAGE_DATABASE_URL", "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix") },
    { name: "jwt-secret", type: "string" as const, default: envString("AGENT_MANAGE_JWT_SECRET", "") },
    { name: "jwt-ttl", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_JWT_TTL", 7 * 86400_000)) },
    { name: "admin-password", type: "string" as const, default: envString("AGENT_MANAGE_ADMIN_PASSWORD", "admin123") },
    { name: "redis-url", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_URL", "") },
    { name: "redis-prefix", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_PREFIX", "ywm") },
    { name: "instance-id", type: "string" as const, default: envString("AGENT_MANAGE_INSTANCE_ID", crypto.randomBytes(6).toString("hex")) },
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
  void argv;
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
    taskTimeoutMs: toMs(values["task-timeout"], 1_800_000),
    databaseURL: values["database-url"],
    jwtSecret: values["jwt-secret"],
    jwtTtlMs: toMs(values["jwt-ttl"], 7 * 86400_000),
    adminPassword: values["admin-password"],
    redisURL: values["redis-url"],
    redisPrefix: values["redis-prefix"],
    instanceID: values["instance-id"],
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
  const loginLimiter = new RateLimiter(10, 60_000); // 每 IP 每分钟 10 次登录尝试
  const uploadLimiter = new RateLimiter(20, 60_000); // 每用户每分钟 20 次上传

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
    }, cfg.redisPrefix);
    await bus.start();
    hub.bus = bus;
    logger.info("redis bus connected", { instance_id: cfg.instanceID });
  }

  const server = http.createServer((req, res) => {
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
    const serveHtml = (file: string) => {
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
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
      const STATIC_MIME: Record<string, string> = { ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp", ".gif": "image/gif", ".ico": "image/x-icon", ".woff": "font/woff", ".woff2": "font/woff2" };
      const name = url.pathname.slice("/static/".length);
      const file = path.resolve(path.dirname(staticFile), name);
      if (name.includes("/") || !file.startsWith(path.dirname(staticFile) + path.sep)) {
        res.writeHead(404).end("not found");
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, { "Content-Type": STATIC_MIME[path.extname(file)] ?? "application/octet-stream", "Cache-Control": "no-cache" });
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
          let name = identity.displayName;
          for (let i = 0; await hub.db.getUserByName(name); i++) {
            name = `${identity.displayName}-${i + 2}`;
          }
          user = {
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
          await hub.db.createUser(user);
          logger.info("oidc user provisioned", { user_id: user.id, name: user.name, employee_id: identity.employeeID });
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
        const ip = clientIp(req.headers, req.socket.remoteAddress);
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
        if (!user || user.disabled === 1 || !body.password || !verifyPassword(body.password, user.password_hash)) {
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
    if (url.pathname === "/attachments" && req.method === "POST") {
      void (async () => {
        if (!attachments) {
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "attachment store not configured" }));
          return;
        }
        const bearer = req.headers.authorization?.startsWith("Bearer ")
          ? req.headers.authorization.slice(7)
          : url.searchParams.get("token") ?? "";
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

  const wss = new WebSocketServer({ noServer: true });

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
    // /ws/admin 仅接受 JWT；/ws/agent 在无 token 时接受设备密钥（?key=）
    const deviceKey = url.pathname === "/ws/agent" && token === ""
      ? url.searchParams.get("key") ?? ""
      : "";
    if (claims === undefined && deviceKey === "") {
      unauthorized();
      return;
    }

    void (async () => {
      let userID: string;
      let deviceKeyID: string | undefined;
      if (claims) {
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
      if (hub.db) {
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
        };
        const ticker = watchPong(ws, agent);
        ws.on("message", (data) => handleAgentMessage(hub, agent, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          if (agent.id !== "") hub.unregisterAgent(agent.id);
        });
        ws.on("error", () => ws.close());
      } else {
        const user: UserConn = { ws, userID, lastHeartbeat: Date.now(), alive: true, isAdmin };
        hub.registerUser(user);
        const ticker = watchPong(ws, user, () => {
          user.lastHeartbeat = Date.now();
        });
        ws.on("message", (data) => handleUserMessage(hub, user, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
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
