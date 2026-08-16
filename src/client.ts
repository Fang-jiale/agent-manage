import os from "node:os";
import path from "node:path";
import http from "node:http";
import { readFileSync, existsSync, promises as fsp } from "node:fs";
import { fileURLToPath } from "node:url";
import { WebSocket } from "ws";
import * as proto from "./protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, setLogLevel, logger } from "./util.ts";
import { HTTPAdapter } from "./adapters/http.ts";
import { StdioAdapter } from "./adapters/stdio.ts";
import { WSAdapter } from "./adapters/ws.ts";
import type { LocalAgentAdapter, LocalAgentEvent } from "./adapters/types.ts";

// 本机覆盖：字符串 = stdio 启动命令（旧格式）；对象 = {conn_type, target}
type LaunchOverride = string | { conn_type?: string; target?: string };

interface ClientConfig {
  gateway: string;
  agentID: string;
  connectorID: string; // 非空即 connector 模式：纯服务，agent 实例由页面分配
  localURL: string;
  adapterType: string;
  token: string;
  deviceKey: string;
  pairCode: string; // 非空即配对模式：凭码换设备密钥并写入配置文件
  configPath: string; // connector 配置文件（零配置启动）
  uiAddr: string; // 本地管理页监听地址，"off" 关闭
  overrides: Record<string, LaunchOverride>; // 本机覆盖：agent_id → 连接方式/目标
  logLevel: string;
  taskTimeoutMs: number;
}

// connector 配置文件：配对成功后写入，之后零参数启动
interface ConnectorFileConfig {
  gateway?: string;
  connector_id?: string;
  key?: string;
  overrides?: Record<string, LaunchOverride>;
}

function loadClientConfig(): ClientConfig {
  const defaultConfigPath = path.join(os.homedir(), ".agent-manage", "connector.json");
  const specs = [
    // gateway 默认空：优先级 CLI > 环境变量 > 配置文件 > 内置默认
    { name: "gateway", type: "string" as const, default: envString("AGENT_MANAGE_GATEWAY", "") },
    { name: "agent-id", type: "string" as const, default: envString("AGENT_MANAGE_AGENT_ID", "") },
    { name: "connector-id", type: "string" as const, default: envString("AGENT_MANAGE_CONNECTOR_ID", "") },
    { name: "local-agent", type: "string" as const, default: envString("AGENT_MANAGE_LOCAL_AGENT", "http://localhost:9001") },
    { name: "adapter", type: "string" as const, default: envString("AGENT_MANAGE_ADAPTER", "http") },
    { name: "token", type: "string" as const, default: envString("AGENT_MANAGE_TOKEN", "") },
    { name: "key", type: "string" as const, default: envString("AGENT_MANAGE_DEVICE_KEY", "") },
    { name: "pair", type: "string" as const, default: envString("AGENT_MANAGE_PAIR_CODE", "") },
    { name: "config", type: "string" as const, default: envString("AGENT_MANAGE_CONFIG", defaultConfigPath) },
    { name: "ui-addr", type: "string" as const, default: envString("AGENT_MANAGE_UI_ADDR", "127.0.0.1:9321") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 1_800_000)) },
  ];
  const values = parseFlags(specs);
  if (values["token"] !== "" && values["key"] !== "") {
    console.error("-token 与 -key 只能二选一");
    process.exit(1);
  }

  // 零配置启动：无显式凭证时读 connector 配置文件
  let fileCfg: ConnectorFileConfig = {};
  if (values["token"] === "" && values["key"] === "") {
    try {
      fileCfg = JSON.parse(readFileSync(values["config"], "utf8")) as ConnectorFileConfig;
    } catch { /* 文件不存在或损坏：按无配置处理 */ }
  }

  const gateway = values["gateway"] || fileCfg.gateway || "ws://localhost:8080/ws/agent";
  const pairCode = values["pair"];
  let connectorID = values["connector-id"] || fileCfg.connector_id || "";
  let deviceKey = values["key"] || fileCfg.key || "";
  if (pairCode !== "") {
    // 配对模式：connector_id 默认主机名；批准后密钥落盘到配置文件
    if (connectorID === "") connectorID = os.hostname();
    deviceKey = "";
  } else if (values["token"] === "" && deviceKey === "") {
    // 单 agent 模式（-agent-id）仍需显式凭证；否则进入未配置态：
    // 本地管理页先起来，在页面上完成配对接入
    if (values["agent-id"] !== "") {
      console.error("缺少认证：-key <设备密钥>（管理后台创建，推荐）或 -token <用户 JWT>（node src/login.ts 获取）");
      process.exit(1);
    }
    logger.info("未找到接入配置，将在本地管理页完成配对", { config: values["config"] });
  }
  let taskTimeoutMs = Number(values["task-timeout"]);
  if (Number.isNaN(taskTimeoutMs)) {
    taskTimeoutMs = parseDurationMs(values["task-timeout"]) ?? 1_800_000;
  }
  return {
    gateway,
    agentID: values["agent-id"],
    connectorID,
    localURL: values["local-agent"],
    adapterType: values["adapter"],
    token: values["token"],
    deviceKey,
    pairCode,
    configPath: values["config"],
    uiAddr: values["ui-addr"],
    overrides: fileCfg.overrides ?? {},
    logLevel: values["log-level"],
    taskTimeoutMs,
  };
}

// 配置文件统一入口：配对落盘与 launch_cmd 覆盖都走这里
async function saveConfig(cfg: ClientConfig): Promise<void> {
  const fileCfg: ConnectorFileConfig = {
    gateway: cfg.gateway,
    connector_id: cfg.connectorID,
    key: cfg.deviceKey,
    overrides: cfg.overrides,
  };
  await fsp.mkdir(path.dirname(cfg.configPath), { recursive: true });
  await fsp.writeFile(cfg.configPath, JSON.stringify(fileCfg, null, 2) + "\n", { mode: 0o600 });
}

function goOS(): string {
  switch (process.platform) {
    case "win32": return "windows";
    default: return process.platform;
  }
}

function goArch(): string {
  switch (process.arch) {
    case "x64": return "amd64";
    case "ia32": return "386";
    default: return process.arch;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// translateLifecycleEvent converts a local agent lifecycle event into the
// corresponding gateway-facing message.
function translateLifecycleEvent(agentID: string, ev: LocalAgentEvent): proto.Message | undefined {
  switch (ev.method) {
    case proto.METHOD_LIFECYCLE_REGISTER: {
      const params = (ev.params ?? {}) as proto.RegisterParams;
      params.agent_id = agentID;
      const ts = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
      return proto.newRequest(`reg-${ts}`, proto.METHOD_REGISTER, params);
    }
    case proto.METHOD_LIFECYCLE_STATUS: {
      const params = (ev.params ?? {}) as proto.LifecycleStatusParams;
      return proto.newNotification(proto.METHOD_STATUS, {
        agent_id: agentID,
        // 本地接口状态词表是 idle/busy/error/offline，网关侧是 online/busy/offline
        status: params.status === "idle" ? "online" : params.status,
        task_id: params.task_id,
        message: params.message,
      } satisfies proto.StatusParams);
    }
    case proto.METHOD_LIFECYCLE_CAPABILITIES_UPDATED: {
      const params = (ev.params ?? {}) as proto.LifecycleCapabilitiesUpdatedParams;
      return proto.newNotification(proto.METHOD_CAPABILITIES_UPDATED, {
        agent_id: agentID,
        capabilities: params.capabilities,
      } satisfies proto.CapabilitiesUpdatedParams);
    }
    default:
      return undefined;
  }
}

function sendProgress(
  ws: WebSocket,
  agentID: string,
  taskID: string,
  sessionID: string | undefined,
  contextID: string | undefined,
  kind: string,
  chunk: proto.LocalAgentChunk,
): void {
  if (!kind) kind = proto.PROGRESS_KIND_REPORT;
  const msg = proto.newNotification(proto.METHOD_PROGRESS, {
    token: taskID,
    value: {
      kind,
      type: chunk.type,
      agent_id: agentID,
      task_id: taskID,
      session_id: sessionID,
      context_id: contextID,
      content: chunk.content,
      name: chunk.name,
      arguments: chunk.arguments,
      confirm_id: chunk.confirm_id,
      prompt_id: chunk.prompt_id,
      options: chunk.options,
      block_id: chunk.block_id,
      blocks: chunk.blocks,
      percentage: chunk.percentage,
      done: chunk.done,
      error: chunk.error,
      reason: chunk.reason,
    } satisfies proto.ProgressValue,
  } satisfies proto.ProgressParams);
  safeSend(ws, msg);
}

function safeSend(ws: WebSocket, msg: proto.Message): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(msg));
}

function sendError(ws: WebSocket, id: string | undefined, code: number, err: unknown): void {
  const message = err instanceof Error ? err.message : String(err);
  safeSend(ws, proto.newErrorResponse(id ?? "", code, message));
}

class TaskRegistry {
  private controllers = new Map<string, { controller: AbortController; timer: NodeJS.Timeout }>();
  private timeoutMs: number;

  constructor(timeoutMs: number) {
    this.timeoutMs = timeoutMs;
  }

  add(taskID: string): AbortController {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort("timeout"), this.timeoutMs);
    timer.unref();
    this.controllers.set(taskID, { controller, timer });
    return controller;
  }

  // get 返回已注册 task 的 controller。handleRespond 用它判断 task 是否仍在
  // 进行（handleChat 还没退出），决定是直接发 task.respond 还是报错。
  get(taskID: string): AbortController | undefined {
    return this.controllers.get(taskID)?.controller;
  }

  cancel(taskID: string): void {
    const entry = this.controllers.get(taskID);
    if (entry) {
      entry.controller.abort();
      this.remove(taskID);
    }
  }

  remove(taskID: string): void {
    const entry = this.controllers.get(taskID);
    if (entry) {
      clearTimeout(entry.timer);
      this.controllers.delete(taskID);
    }
  }

  size(): number {
    return this.controllers.size;
  }

  cancelAll(): void {
    for (const [id] of this.controllers) this.cancel(id);
  }
}

function sendStatus(ws: WebSocket, agentID: string, status: string, taskID?: string): void {
  safeSend(ws, proto.newNotification(proto.METHOD_STATUS, {
    agent_id: agentID,
    status,
    task_id: taskID,
  } satisfies proto.StatusParams));
}

// updateBusyStatus 按 tasks.size 切换 agent 状态：还有 in-flight task 就 BUSY，
// 否则 ONLINE。多个 task 并发时，先结束的那个不会误报 ONLINE。
function updateBusyStatus(ws: WebSocket, agentID: string, tasks: TaskRegistry): void {
  if (tasks.size() > 0) {
    sendStatus(ws, agentID, proto.AGENT_STATUS_BUSY);
  } else {
    sendStatus(ws, agentID, proto.AGENT_STATUS_ONLINE);
  }
}

async function handleChat(
  ws: WebSocket,
  adapter: LocalAgentAdapter,
  agentID: string,
  msg: proto.Message,
  tasks: TaskRegistry,
): Promise<void> {
  const params = proto.decodeParams<proto.AgentChatParams>(msg);

  safeSend(ws, proto.newResponse(msg.id ?? "", {
    status: "accepted",
    task_id: params.task_id,
  } satisfies proto.TaskAcceptResult));

  const controller = tasks.add(params.task_id);
  sendStatus(ws, agentID, proto.AGENT_STATUS_BUSY, params.task_id);
  try {
    const req: proto.LocalAgentRequest = {
      task_id: params.task_id,
      session_id: params.session_id,
      context_id: params.context_id,
      type: "chat",
      content: params.content,
      metadata: params.metadata,
    };

    let chunks: AsyncIterable<proto.LocalAgentChunk>;
    try {
      chunks = await adapter.send(req, controller.signal);
    } catch (err) {
      const errMsg = controller.signal.aborted && controller.signal.reason === "timeout"
        ? "timeout"
        : err instanceof Error ? err.message : String(err);
      sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
        proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: errMsg, done: true });
      return;
    }

    // 跨 task.respond 轮次持续消费同一 queue。confirm_required 不再提前关 queue，
    // 后续 task.respond 触发的 chunk 流回这里继续转发给网关。
    for await (const chunk of chunks) {
      const kind = chunk.done ? proto.PROGRESS_KIND_END : proto.PROGRESS_KIND_REPORT;
      sendProgress(ws, agentID, chunk.task_id ?? params.task_id, chunk.session_id ?? params.session_id,
        chunk.context_id ?? params.context_id, kind, chunk);
    }
    if (controller.signal.aborted && controller.signal.reason === "timeout") {
      sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
        proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: "timeout", done: true });
    }
  } finally {
    tasks.remove(params.task_id);
    updateBusyStatus(ws, agentID, tasks);
  }
}

async function handleRespond(
  ws: WebSocket,
  adapter: LocalAgentAdapter,
  agentID: string,
  msg: proto.Message,
  tasks: TaskRegistry,
): Promise<void> {
  let params: proto.AgentRespondParams;
  try {
    params = proto.decodeParams<proto.AgentRespondParams>(msg);
  } catch (err) {
    sendError(ws, msg.id, proto.ERR_INVALID_PARAMS, err);
    return;
  }

  // task.respond 复用 handleChat 注册的 controller；不再自己 add（避免覆盖原
  // controller、避免 size 计数错乱）。task 不存在说明 handleChat 已退出（task
  // 超时 / cancel / agent 重启），respond 已无意义。
  const controller = tasks.get(params.task_id);
  if (!controller) {
    if (msg.id) {
      safeSend(ws, proto.newErrorResponse(msg.id, proto.ERR_AGENT_NOT_FOUND,
        `task ${params.task_id} not in flight`));
    }
    return;
  }

  const req: proto.LocalAgentRequest = {
    task_id: params.task_id,
    session_id: params.session_id,
    type: "respond",
    confirm_id: params.confirm_id,
    prompt_id: params.prompt_id,
    block_id: params.block_id,
    action_id: params.action_id,
    response: params.response,
  };

  // 把 task.respond 写到 shim stdin；不迭代返回的 queue——chunk 流由 handleChat
  // 的 for-await 继续消费（queue 按 task_id 复用）。这里只关心把请求送出去 + 把
  // 同步可得的 result 回给 gateway。
  try {
    await adapter.send(req, controller.signal);
  } catch (err) {
    const errMsg = controller.signal.aborted && controller.signal.reason === "timeout"
      ? "timeout"
      : err instanceof Error ? err.message : String(err);
    sendProgress(ws, agentID, params.task_id, params.session_id, undefined,
      proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: errMsg, done: true });
    return;
  }

  // 把 task.respond 的 result 透传给 browser（rule ③：status:"accepted" + decision）
  // decision 归一：旧 shim 可能传 boolean/string，按 RESPOND_DECISION_* 归一映射。
  if (msg.id) {
    safeSend(ws, proto.newResponse(msg.id, {
      task_id: params.task_id,
      session_id: params.session_id,
      confirm_id: params.confirm_id,
      status: "accepted",
      decision: normalizeDecision(params.response),
    } satisfies proto.TaskRespondResult));
  }
}

function normalizeDecision(response: unknown): "allow" | "deny" | "cancel" | undefined {
  if (response && typeof response === "object") {
    const d = (response as { decision?: unknown }).decision;
    if (d === proto.RESPOND_DECISION_ALLOW || d === proto.RESPOND_DECISION_DENY || d === proto.RESPOND_DECISION_CANCEL) return d;
  }
  if (response === true) return proto.RESPOND_DECISION_ALLOW;
  if (response === false) return proto.RESPOND_DECISION_DENY;
  if (response === proto.RESPOND_DECISION_ALLOW || response === proto.RESPOND_DECISION_DENY || response === proto.RESPOND_DECISION_CANCEL) {
    return response as "allow" | "deny" | "cancel";
  }
  return undefined;
}

// ---- 管理者编排桥接：本地 Agent task.invoke ⇄ 网关 agent.task.invoke ----

interface PendingInvoke {
  adapter: LocalAgentAdapter;
  agentMsgID: string; // 本地 Agent 请求的 id，网关响应后按此回写
}

// 本地 Agent 发来 task.invoke（请求）→ 转成网关 agent.task.invoke。
// 鉴权（管理者身份、同群目标、防递归）全部在网关做，client 只透传结果。
function forwardLocalInvoke(
  ws: WebSocket, agentID: string, adapter: LocalAgentAdapter,
  ev: LocalAgentEvent, pendingInvokes: Map<string, PendingInvoke>,
): void {
  const reply = (result?: unknown, err?: [number, string]): void => {
    if (!ev.id) return;
    adapter.sendToAgent?.(err
      ? proto.newErrorResponse(ev.id, err[0], err[1])
      : proto.newResponse(ev.id, result));
  };
  if (!ev.id) return;
  const p = (ev.params ?? {}) as {
    parent_task_id?: string; group_id?: string; target_agent_id?: string;
    type?: string; content?: string; metadata?: Record<string, unknown>;
  };
  if (!p.parent_task_id || !p.group_id || !p.target_agent_id) {
    reply(undefined, [proto.ERR_INVALID_PARAMS, "parent_task_id / group_id / target_agent_id are required"]);
    return;
  }
  const rpcID = `inv-${agentID}-${ev.id}`;
  pendingInvokes.set(rpcID, { adapter, agentMsgID: ev.id });
  safeSend(ws, proto.newRequest(rpcID, proto.METHOD_AGENT_TASK_INVOKE, {
    parent_task_id: p.parent_task_id,
    group_id: p.group_id,
    target_agent_id: p.target_agent_id,
    type: p.type || "chat",
    content: p.content ?? "",
    metadata: p.metadata,
  } satisfies proto.AgentTaskInvokeParams));
}

// 网关对 agent.task.invoke 的响应 → 回给本地 Agent（result/error 原样透传）。
// 返回 true 表示该消息是桥接的 invoke 响应，已消费。
function resolveLocalInvoke(msg: proto.Message, pendingInvokes: Map<string, PendingInvoke>): boolean {
  if (!msg.id || !pendingInvokes.has(msg.id)) return false;
  const p = pendingInvokes.get(msg.id)!;
  pendingInvokes.delete(msg.id);
  p.adapter.sendToAgent?.(msg.error
    ? proto.newErrorResponse(p.agentMsgID, msg.error.code, msg.error.message)
    : proto.newResponse(p.agentMsgID, msg.result));
  return true;
}

// 网关连接断开：给未决 invoke 回错误，避免本地 Agent 挂等
function failLocalInvokes(pendingInvokes: Map<string, PendingInvoke>, reason: string): void {
  for (const p of pendingInvokes.values()) {
    p.adapter.sendToAgent?.(proto.newErrorResponse(p.agentMsgID, proto.ERR_INTERNAL_ERROR, reason));
  }
  pendingInvokes.clear();
}

// 网关 agent.task.result（notification）→ 本地 Agent task.subtask_result
function forwardSubtaskResult(adapter: LocalAgentAdapter, params: unknown): void {
  adapter.sendToAgent?.(proto.newNotification(proto.METHOD_TASK_SUBTASK_RESULT, params));
}

// 事件循环的统一入口：编排请求走桥接，其余按生命周期消息翻译。
// 返回翻译后的网关消息（无需上送时返回 undefined）。
function bridgeAgentEvent(
  ws: WebSocket | null, agentID: string, adapter: LocalAgentAdapter,
  ev: LocalAgentEvent, pendingInvokes: Map<string, PendingInvoke>,
): proto.Message | undefined {
  if (ev.method === proto.METHOD_TASK_INVOKE) {
    if (!ws) {
      if (ev.id) adapter.sendToAgent?.(proto.newErrorResponse(ev.id, proto.ERR_INTERNAL_ERROR, "gateway not connected"));
      return undefined;
    }
    forwardLocalInvoke(ws, agentID, adapter, ev, pendingInvokes);
    return undefined;
  }
  return translateLifecycleEvent(agentID, ev);
}

function sendRegister(ws: WebSocket, agentID: string, caps: proto.Capability[]): void {
  if (caps.length === 0) {
    caps = [{ type: "chat", name: "general", description: "通用对话能力" }];
  }
  safeSend(ws, proto.newRequest("register-1", proto.METHOD_REGISTER, {
    agent_id: agentID,
    name: agentID,
    version: "1.0.0",
    capabilities: caps,
    platform: {
      os: goOS(),
      arch: goArch(),
      hostname: agentID,
    },
  } satisfies proto.RegisterParams));
}

// ---- connector 模式：client 只起服务，agent 实例由页面分配（connector.sync 全量对账） ----

interface HostedAgent {
  adapter: LocalAgentAdapter;
  tasks: TaskRegistry;
  brandID: string;
  name: string;
  capabilities: proto.Capability[];
  connType: string; // 生效连接方式（stdio|http|ws）；变了要重建 adapter
  target: string; // 生效目标（启动命令或服务地址）
}

// 解析生效连接方式与目标：本机覆盖 > 品牌下发 > -adapter/-local-agent 兜底
function resolveLaunch(cfg: ClientConfig, a: proto.ConnectorSyncAgent): { connType: string; target: string } {
  const o = cfg.overrides[a.agent_id];
  const oType = typeof o === "object" && o !== null ? o.conn_type : undefined;
  const oTarget = typeof o === "string" ? o : o?.target;
  let connType = oType ?? a.conn_type ?? "";
  let target = oTarget
    ?? (connType === "http" || connType === "ws" ? a.endpoint : a.launch_cmd)
    ?? "";
  if (connType === "") connType = a.launch_cmd ? "stdio" : (a.endpoint ? "http" : "");
  if (connType === "" || target === "") {
    connType = cfg.adapterType;
    target = cfg.localURL;
  }
  return { connType, target };
}

async function createAdapter(connType: string, target: string): Promise<LocalAgentAdapter> {
  switch (connType) {
    case "stdio":
      return StdioAdapter.create(target);
    case "ws":
      return WSAdapter.create(target);
    default:
      return HTTPAdapter.create(target);
  }
}

// ---- 本地管理页共享状态：UI HTTP 服务与 connector 主循环经它交互 ----

interface LocalUIState {
  connected: boolean;
  lastSync: proto.ConnectorSyncAgent[]; // 最近一次全量目标集
  hosted: Map<string, HostedAgent> | undefined;
  rpc: (method: string, params: object) => Promise<proto.Message>;
  applyOverride: (agentID: string, override: LaunchOverride | "") => Promise<void>; // 空串 = 清除覆盖
  dropOverride: (agentID: string) => Promise<void>;
  pairAndConnect?: (gateway: string, code: string, connectorID?: string) => Promise<void>;
}

function newLocalUIState(): LocalUIState {
  return {
    connected: false,
    lastSync: [],
    hosted: undefined,
    rpc: () => Promise.reject(new Error("connector 未运行")),
    applyOverride: () => Promise.reject(new Error("connector 未运行")),
    dropOverride: () => Promise.reject(new Error("connector 未运行")),
  };
}

// 返回 true 表示被新实例顶替（4002），调用方应直接退出进程
async function mainConnector(cfg: ClientConfig, connectorID: string, ui: LocalUIState): Promise<boolean> {
  const hosted = new Map<string, HostedAgent>();
  let currentWS: WebSocket | null = null;
  const platform: proto.PlatformInfo = { os: goOS(), arch: goArch(), hostname: os.hostname() };
  const pendingRPC = new Map<string, { resolve: (m: proto.Message) => void; timer: NodeJS.Timeout }>();
  const pendingInvokes = new Map<string, PendingInvoke>();

  ui.hosted = hosted;
  ui.rpc = (method, params) => new Promise((resolve, reject) => {
    const ws = currentWS;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      reject(new Error("gateway not connected"));
      return;
    }
    const id = `ui-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const timer = setTimeout(() => {
      pendingRPC.delete(id);
      reject(new Error("rpc timeout"));
    }, 10_000);
    pendingRPC.set(id, { resolve, timer });
    safeSend(ws, proto.newRequest(id, method, params));
  });
  ui.applyOverride = async (agentID, override) => {
    if (override === "") delete cfg.overrides[agentID];
    else cfg.overrides[agentID] = override;
    await saveConfig(cfg);
    // 立即重建该实例（ensureAgent 检测连接方式/目标变化）；未托管则下次 sync 生效
    const target = ui.lastSync.find((x) => x.agent_id === agentID);
    if (target) await ensureAgent(target);
  };
  ui.dropOverride = async (agentID) => {
    if (delete cfg.overrides[agentID]) await saveConfig(cfg);
  };
  function rejectPendingRPC(reason: string): void {
    for (const [id, p] of pendingRPC) {
      clearTimeout(p.timer);
      pendingRPC.delete(id);
      p.resolve({ jsonrpc: proto.VERSION, id, error: { code: proto.ERR_INTERNAL_ERROR, message: reason } });
    }
  }

  function registerToGateway(agentID: string, h: HostedAgent): void {
    if (!currentWS) return;
    safeSend(currentWS, proto.newRequest(`reg-${agentID}-${Date.now()}`, proto.METHOD_REGISTER, {
      agent_id: agentID,
      name: h.name,
      capabilities: h.capabilities,
      brand_id: h.brandID,
      platform,
    } satisfies proto.RegisterParams));
  }

  async function ensureAgent(a: proto.ConnectorSyncAgent): Promise<void> {
    const { connType, target } = resolveLaunch(cfg, a);
    let h = hosted.get(a.agent_id);
    if (h && (h.connType !== connType || h.target !== target)) {
      // 连接方式/目标变更：停掉旧本地服务，按新配置重建
      logger.info("agent launch changed, rebuilding", { agent_id: a.agent_id });
      await dropAgent(a.agent_id);
      h = undefined;
    }
    if (!h) {
      const adapter = await createAdapter(connType, target);
      h = {
        adapter, tasks: new TaskRegistry(cfg.taskTimeoutMs), brandID: a.brand_id,
        name: a.name, capabilities: a.capabilities, connType, target,
      };
      hosted.set(a.agent_id, h);
      // 生命周期桥接：register 由 sync 数据驱动（忽略 shim 的 register，防覆盖品牌约束），其余透传
      const events = adapter.events();
      if (events) {
        void (async () => {
          for await (const ev of events) {
            if (ev.method === proto.METHOD_LIFECYCLE_REGISTER) continue;
            const m = bridgeAgentEvent(currentWS, a.agent_id, adapter, ev, pendingInvokes);
            if (m && currentWS) safeSend(currentWS, m);
          }
        })();
      }
      logger.info("agent hosted", { agent_id: a.agent_id, brand_id: a.brand_id, launch: target });
    }
    registerToGateway(a.agent_id, h);
  }

  async function dropAgent(agentID: string): Promise<void> {
    const h = hosted.get(agentID);
    if (!h) return;
    hosted.delete(agentID);
    h.tasks.cancelAll();
    h.adapter.close();
    logger.info("agent dropped", { agent_id: agentID });
  }

  // 全量对账：目标集里没有的下线，缺的上起并注册；已有的重注册（重连恢复）
  function applySync(params: proto.ConnectorSyncParams): void {
    ui.lastSync = params.agents ?? [];
    const want = new Map((params.agents ?? []).map((a) => [a.agent_id, a]));
    for (const id of [...hosted.keys()]) {
      if (!want.has(id)) void dropAgent(id);
    }
    for (const a of want.values()) {
      void ensureAgent(a).catch((e) => logger.error("agent host failed", { agent_id: a.agent_id, error: String(e) }));
    }
  }

  const gatewayURL = new URL(cfg.gateway);
  if (cfg.deviceKey !== "") {
    gatewayURL.searchParams.set("key", cfg.deviceKey);
  } else {
    gatewayURL.searchParams.set("token", cfg.token);
  }

  let interrupted = false;
  process.on("SIGINT", () => {
    interrupted = true;
  });
  // SIGTERM（kill 默认信号）同样走优雅退出：否则 stdio 子进程全部变孤儿
  process.on("SIGTERM", () => {
    interrupted = true;
  });

  let reconnectDelay = 1000;
  while (!interrupted) {
    logger.info("connecting to gateway", { url: gatewayURL.toString(), connector_id: connectorID });

    const ws = await new Promise<WebSocket | null>((resolve) => {
      const conn = new WebSocket(gatewayURL.toString());
      conn.once("open", () => resolve(conn));
      conn.once("error", () => resolve(null));
    });

    if (interrupted) break;
    if (!ws) {
      logger.warn("dial failed", { retry_in: `${reconnectDelay}ms` });
      await sleep(reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
      continue;
    }
    reconnectDelay = 1000;
    currentWS = ws;
    ui.connected = true;

    // connector 报到；网关应答后推 connector.sync（含断线重连恢复）
    safeSend(ws, proto.newRequest("hello-1", proto.METHOD_CONNECTOR_HELLO, {
      connector_id: connectorID,
      platform,
    } satisfies proto.ConnectorHelloParams));

    const heartbeat = setInterval(() => {
      // connector 级心跳：网关按 ws 给所有托管 agent 续命
      safeSend(ws, proto.newNotification(proto.METHOD_HEARTBEAT, {
        timestamp: proto.rfc3339Now(),
      } satisfies proto.HeartbeatParams));
      // 每 agent 状态自愈（同单 agent 模式）
      for (const [id, h] of hosted) updateBusyStatus(ws, id, h.tasks);
    }, 30_000);
    heartbeat.unref();

    ws.on("message", (data) => {
      let msg: proto.Message;
      try {
        msg = JSON.parse(data.toString()) as proto.Message;
      } catch {
        return;
      }
      // 本地管理页经 agent 通道发的 RPC 响应
      if (msg.id) {
        const p = pendingRPC.get(msg.id);
        if (p) {
          pendingRPC.delete(msg.id);
          clearTimeout(p.timer);
          p.resolve(msg);
          return;
        }
      }
      if (resolveLocalInvoke(msg, pendingInvokes)) return;
      switch (msg.method) {
        case proto.METHOD_CONNECTOR_SYNC:
          applySync(proto.decodeParams<proto.ConnectorSyncParams>(msg));
          break;
        case proto.METHOD_CONNECTOR_RESTART: {
          // 重启单实例：杀子进程后按原目标重建，agent_id 不变（程序更新场景）
          const p = proto.decodeParams<proto.ConnectorRestartParams>(msg);
          const target = ui.lastSync.find((x) => x.agent_id === p.agent_id);
          if (!hosted.has(p.agent_id)) break; // 未托管实例：sync 会按需拉起
          void (async () => {
            logger.info("agent restarting", { agent_id: p.agent_id });
            await dropAgent(p.agent_id);
            if (target) await ensureAgent(target);
          })().catch((e) => logger.error("agent restart failed", { agent_id: p.agent_id, error: String(e) }));
          break;
        }
        case proto.METHOD_AGENT_TASK_RESULT: {
          // 编排子任务结果 → 管理者实例（agent_id 指明接收方）
          const p = (msg.params ?? {}) as { agent_id?: string };
          const h = hosted.get(p.agent_id ?? "");
          if (h) forwardSubtaskResult(h.adapter, msg.params);
          break;
        }
        case proto.METHOD_AGENT_CHAT: {
          const p = proto.decodeParams<proto.AgentChatParams>(msg);
          const h = hosted.get(p.agent_id ?? "");
          if (!h) {
            sendError(ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not hosted by this connector");
            break;
          }
          void handleChat(ws, h.adapter, p.agent_id ?? "", msg, h.tasks);
          break;
        }
        case proto.METHOD_AGENT_RESPOND: {
          const p = proto.decodeParams<proto.AgentRespondParams>(msg);
          const h = hosted.get(p.agent_id ?? "");
          if (!h) {
            sendError(ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not hosted by this connector");
            break;
          }
          void handleRespond(ws, h.adapter, p.agent_id ?? "", msg, h.tasks);
          break;
        }
        case proto.METHOD_AGENT_CANCEL: {
          const p = proto.decodeParams<proto.AgentCancelParams>(msg);
          const h = hosted.get(p.agent_id ?? "");
          if (h) h.tasks.cancel(p.task_id);
          if (msg.id) {
            safeSend(ws, proto.newResponse(msg.id, {
              task_id: p.task_id,
              status: "cancelled",
            } satisfies proto.TaskCancelResult));
          }
          break;
        }
      }
    });

    const closeCode = await new Promise<number>((resolve) => {
      ws.once("close", (code) => resolve(code));
      const onSigint = (): void => {
        ws.close(1000);
        resolve(1000);
      };
      process.once("SIGINT", onSigint);
      process.once("SIGTERM", onSigint);
    });

    clearInterval(heartbeat);
    currentWS = null;
    ui.connected = false;
    rejectPendingRPC("connection lost");
    failLocalInvokes(pendingInvokes, "gateway connection lost");
    if (interrupted) {
      logger.info("interrupt received, exiting...");
      break;
    }
    if (closeCode === 4002) {
      // 被同 connector_id 的新实例顶替：退出而不是重连，避免两个实例互踢
      logger.error("replaced by a newer instance (duplicate connector_id), exiting");
      for (const id of [...hosted.keys()]) await dropAgent(id);
      return true;
    }
    logger.info("connection lost, reconnecting...");
    for (const h of hosted.values()) h.tasks.cancelAll();
    await sleep(reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  for (const id of [...hosted.keys()]) await dropAgent(id);
  return false;
}

// ---- 本地管理页 HTTP 服务（绑回环，不鉴权）----

function uiPagePath(): string {
  const here = path.dirname(fileURLToPath(import.meta.url));
  for (const p of [
    path.resolve(here, "static", "client.html"), // 打包布局：client.mjs 同级 static/
    path.resolve(here, "..", "static", "client.html"), // 源码布局：src/ 上一级 static/
  ]) {
    if (existsSync(p)) return p;
  }
  return path.resolve(here, "..", "static", "client.html");
}

function readBody(req: http.IncomingMessage): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      try {
        resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown> : {});
      } catch {
        reject(new Error("invalid json body"));
      }
    });
    req.on("error", reject);
  });
}

function sendJSON(res: http.ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

function startLocalUI(addr: string, cfg: ClientConfig, ui: LocalUIState): http.Server {
  const idx = addr.lastIndexOf(":");
  const host = idx === -1 ? "127.0.0.1" : addr.slice(0, idx) || "127.0.0.1";
  const port = idx === -1 ? Number(addr) : Number(addr.slice(idx + 1));
  const server = http.createServer((req, res) => {
    void handleUIRequest(req, res, cfg, ui).catch((e) => {
      sendJSON(res, 500, { error: e instanceof Error ? e.message : String(e) });
    });
  });
  let stopped = false;
  server.once("close", () => {
    stopped = true;
  });
  server.on("error", (e) => {
    // 端口可能被一个即将被顶替退出的旧实例占用：每隔 3s 重试，旧实例退出后抢回
    if ((e as NodeJS.ErrnoException).code === "EADDRINUSE") {
      logger.warn("本地管理页端口被占，3s 后重试", { addr });
      const t = setTimeout(() => {
        if (!stopped) server.listen(port, host);
      }, 3000);
      t.unref();
      return;
    }
    logger.error("本地管理页监听失败", { addr, error: String(e) });
  });
  server.listen(port, host, () => logger.info("本地管理页已启动", { url: `http://${host}:${port}` }));
  return server;
}

async function handleUIRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  cfg: ClientConfig,
  ui: LocalUIState,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const p = url.pathname;
  if (req.method === "GET" && (p === "/" || p === "/index.html")) {
    const html = await fsp.readFile(uiPagePath(), "utf8");
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
    return;
  }
  if (req.method === "GET" && p === "/api/state") {
    const agents = ui.lastSync.map((a) => {
      const { connType, target } = resolveLaunch(cfg, a);
      return {
        agent_id: a.agent_id,
        brand_id: a.brand_id,
        name: a.name,
        conn_type: connType,
        launch_cmd: target,
        launch_cmd_source: cfg.overrides[a.agent_id] !== undefined
          ? "override"
          : ((a.launch_cmd ?? a.endpoint) ? "brand" : "local"),
        running: ui.hosted?.has(a.agent_id) ?? false,
      };
    });
    sendJSON(res, 200, {
      configured: cfg.deviceKey !== "" || cfg.token !== "",
      connected: ui.connected,
      gateway: cfg.gateway,
      connector_id: cfg.connectorID,
      config_path: cfg.configPath,
      agents,
    });
    return;
  }
  if (req.method === "POST" && p === "/api/pair") {
    if (!ui.pairAndConnect) {
      sendJSON(res, 409, { error: `已完成配置；如需重新接入请删除 ${cfg.configPath} 后重启` });
      return;
    }
    const body = await readBody(req);
    const code = String(body.code ?? "").trim();
    if (!code) {
      sendJSON(res, 400, { error: "code required" });
      return;
    }
    await ui.pairAndConnect(
      String(body.gateway ?? "").trim(),
      code,
      String(body.connector_id ?? "").trim() || undefined,
    );
    sendJSON(res, 200, { status: "ok" });
    return;
  }
  if (req.method === "GET" && p === "/api/brands") {
    const r = await ui.rpc(proto.METHOD_BRAND_LIST, {});
    if (r.error) {
      sendJSON(res, 502, { error: r.error.message });
      return;
    }
    sendJSON(res, 200, r.result);
    return;
  }
  if (req.method === "POST" && p === "/api/agents") {
    const body = await readBody(req);
    const brandID = String(body.brand_id ?? "").trim();
    if (!brandID) {
      sendJSON(res, 400, { error: "brand_id required" });
      return;
    }
    const r = await ui.rpc(proto.METHOD_AGENT_ASSIGN, {
      connector_id: cfg.connectorID,
      brand_id: brandID,
      name: String(body.name ?? "").trim() || undefined,
    } satisfies proto.AgentAssignParams);
    if (r.error) {
      sendJSON(res, 502, { error: r.error.message });
      return;
    }
    const agentID = (r.result as proto.AgentAssignResult).agent_id;
    const cmd = String(body.target ?? body.launch_cmd ?? "").trim();
    if (cmd) {
      const ct = String(body.conn_type ?? "").trim();
      await ui.applyOverride(agentID, ct !== "" ? { conn_type: ct, target: cmd } : cmd);
    }
    sendJSON(res, 200, { agent_id: agentID });
    return;
  }
  const m = /^\/api\/agents\/([^/]+)$/.exec(p);
  if (m && req.method === "PUT") {
    const body = await readBody(req);
    const target = String(body.target ?? body.launch_cmd ?? "").trim();
    const ct = String(body.conn_type ?? "").trim();
    if (target === "" && ct === "") {
      await ui.applyOverride(m[1], ""); // 清除覆盖
    } else {
      await ui.applyOverride(m[1], ct !== "" ? { conn_type: ct, target } : target);
    }
    sendJSON(res, 200, { status: "ok" });
    return;
  }
  if (m && req.method === "DELETE") {
    const r = await ui.rpc(proto.METHOD_AGENT_REMOVE, { agent_id: m[1] } satisfies proto.AgentRemoveParams);
    if (r.error) {
      sendJSON(res, 502, { error: r.error.message });
      return;
    }
    await ui.dropOverride(m[1]);
    sendJSON(res, 200, { status: "ok" });
    return;
  }
  sendJSON(res, 404, { error: "not found" });
}

// ---- 配对模式：凭一次性配对码换设备密钥，落盘后进入 connector 模式 ----

async function runPairing(cfg: ClientConfig): Promise<void> {
  const gatewayURL = new URL(cfg.gateway);
  gatewayURL.searchParams.set("pair", "1");
  logger.info("pairing with gateway", { url: cfg.gateway, connector_id: cfg.connectorID });

  const ws = new WebSocket(gatewayURL.toString());
  const key = await new Promise<string>((resolve, reject) => {
    ws.once("open", () => {
      safeSend(ws, proto.newRequest("pair-1", proto.METHOD_CONNECTOR_PAIR, {
        code: cfg.pairCode,
        connector_id: cfg.connectorID,
        platform: { os: goOS(), arch: goArch(), hostname: os.hostname() },
        version: "1.0.0",
      } satisfies proto.ConnectorPairParams));
    });
    ws.on("message", (data) => {
      let msg: proto.Message;
      try {
        msg = JSON.parse(data.toString()) as proto.Message;
      } catch {
        return;
      }
      if (msg.method === proto.METHOD_CONNECTOR_CREDENTIAL) {
        const p = (msg.params ?? {}) as proto.ConnectorCredentialParams;
        resolve(p.key);
        return;
      }
      if (msg.id === "pair-1") {
        const err = (msg as { error?: { message?: string } }).error;
        if (err) {
          reject(new Error(err.message ?? "pairing failed"));
          return;
        }
        const r = (msg.result ?? {}) as proto.ConnectorPairResult;
        if (r.status === "pending") {
          logger.info("配对请求已受理，等待管理员在后台审批...", { connector_id: cfg.connectorID });
        }
      }
    });
    ws.on("close", (code, reason) => {
      reject(new Error(`connection closed (${code}): ${reason.toString() || "no reason"}`));
    });
    ws.on("error", (e) => reject(e));
  });

  // 凭证到手：写配置文件（0600），之后零参数启动
  cfg.deviceKey = key;
  await saveConfig(cfg);
  logger.info("配对成功，凭证已写入配置文件", { path: cfg.configPath, connector_id: cfg.connectorID });
  ws.close();
}

async function main(): Promise<void> {
  const cfg = loadClientConfig();
  setLogLevel(cfg.logLevel);

  // 本地管理页：无论是否已配置都先起来
  const ui = newLocalUIState();
  const uiServer = cfg.uiAddr !== "off" ? startLocalUI(cfg.uiAddr, cfg, ui) : undefined;

  // 配对模式：凭码换密钥并落盘，随后直接进入 connector 模式
  if (cfg.pairCode !== "") {
    await runPairing(cfg);
  } else if (cfg.connectorID === "" && cfg.deviceKey === "" && cfg.token === "" && cfg.agentID === "") {
    // 未配置：本地页已可访问，等页面配对完成后继续
    logger.info("未配置接入信息，请在本地管理页完成配对", { config: cfg.configPath });
    await new Promise<void>((resolve) => {
      ui.pairAndConnect = async (gateway, code, connectorID) => {
        if (gateway) cfg.gateway = gateway;
        cfg.connectorID = connectorID ?? os.hostname();
        cfg.pairCode = code;
        await runPairing(cfg); // 失败抛错给页面，可重试
        resolve();
      };
    });
    ui.pairAndConnect = undefined;
  }

  // connector 模式：只起服务，agent 由页面分配
  if (cfg.connectorID !== "") {
    const replaced = await mainConnector(cfg, cfg.connectorID, ui);
    uiServer?.close();
    // 被顶替属异常退出（非 0），让 supervisor/日志能看到双实例冲突
    if (replaced) process.exit(1);
    return;
  }
  if (cfg.agentID === "") {
    cfg.agentID = os.hostname();
  }

  let adapter: LocalAgentAdapter;
  switch (cfg.adapterType) {
    case "stdio":
      adapter = await StdioAdapter.create(cfg.localURL);
      break;
    case "http":
      adapter = await HTTPAdapter.create(cfg.localURL);
      break;
    default:
      logger.error("unknown adapter", { adapter: cfg.adapterType });
      process.exit(1);
  }

  const gatewayURL = new URL(cfg.gateway);
  if (cfg.deviceKey !== "") {
    gatewayURL.searchParams.set("key", cfg.deviceKey);
  } else {
    gatewayURL.searchParams.set("token", cfg.token);
  }

  const tasks = new TaskRegistry(cfg.taskTimeoutMs);
  let interrupted = false;
  let replaced = false;
  process.on("SIGINT", () => {
    interrupted = true;
  });
  process.on("SIGTERM", () => {
    interrupted = true;
  });

  // Bridge lifecycle events from the local agent to the current gateway connection.
  let currentWS: WebSocket | null = null;
  const pendingInvokes = new Map<string, PendingInvoke>();
  const events = adapter.events();
  if (events) {
    void (async () => {
      for await (const ev of events) {
        const msg = bridgeAgentEvent(currentWS, cfg.agentID, adapter, ev, pendingInvokes);
        if (msg && currentWS) safeSend(currentWS, msg);
      }
    })();
  }

  let reconnectDelay = 1000;
  while (!interrupted) {
    logger.info("connecting to gateway", { url: gatewayURL.toString() });

    const ws = await new Promise<WebSocket | null>((resolve) => {
      const conn = new WebSocket(gatewayURL.toString());
      conn.once("open", () => resolve(conn));
      conn.once("error", () => resolve(null));
    });

    if (interrupted) break;
    if (!ws) {
      logger.warn("dial failed", { retry_in: `${reconnectDelay}ms` });
      await sleep(reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
      continue;
    }
    reconnectDelay = 1000;
    currentWS = ws;

    sendRegister(ws, cfg.agentID, adapter.getCapabilities());

    const heartbeat = setInterval(() => {
      safeSend(ws, proto.newNotification(proto.METHOD_HEARTBEAT, {
        agent_id: cfg.agentID,
        timestamp: proto.rfc3339Now(),
      } satisfies proto.HeartbeatParams));
      // 自愈：shim 的 error 状态是粘性的（任务异常后不一定补发 idle），
      // 心跳时按 in-flight 任务数重报一次状态，避免 error 卡死展示
      updateBusyStatus(ws, cfg.agentID, tasks);
    }, 30_000);
    heartbeat.unref();

    ws.on("message", (data) => {
      let msg: proto.Message;
      try {
        msg = JSON.parse(data.toString()) as proto.Message;
      } catch {
        return;
      }
      if (resolveLocalInvoke(msg, pendingInvokes)) return;
      switch (msg.method) {
        case proto.METHOD_AGENT_CHAT:
          void handleChat(ws, adapter, cfg.agentID, msg, tasks);
          break;
        case proto.METHOD_AGENT_RESPOND:
          void handleRespond(ws, adapter, cfg.agentID, msg, tasks);
          break;
        case proto.METHOD_AGENT_TASK_RESULT:
          // 编排子任务结果 → 本地 Agent（task.subtask_result）
          forwardSubtaskResult(adapter, msg.params);
          break;
        case proto.METHOD_AGENT_CANCEL: {
          const params = proto.decodeParams<proto.AgentCancelParams>(msg);
          // tasks.cancel 触发 controller.abort，adapter 内的 abort 回调会把
          // task.cancel 转发到 shim。不再需要 adapter.cancelTask 显式调用——
          // 新架构下 controller 在 done:true 之前一直存活，abort 必定触发。
          tasks.cancel(params.task_id);
          if (msg.id) {
            safeSend(ws, proto.newResponse(msg.id, {
              task_id: params.task_id,
              status: "cancelled",
            } satisfies proto.TaskCancelResult));
          }
          break;
        }
      }
    });

    const closeCode = await new Promise<number>((resolve) => {
      ws.once("close", (code) => resolve(code));
      const onSigint = (): void => {
        ws.close(1000);
        resolve(1000);
      };
      process.once("SIGINT", onSigint);
      process.once("SIGTERM", onSigint);
    });

    clearInterval(heartbeat);
    currentWS = null;
    failLocalInvokes(pendingInvokes, "gateway connection lost");
    if (interrupted) {
      logger.info("interrupt received, exiting...");
      break;
    }
    if (closeCode === 4002) {
      // 被同 agent_id 的新实例顶替：退出而不是重连，避免两个实例互踢
      logger.error("replaced by a newer instance (duplicate agent_id), exiting");
      replaced = true;
      break;
    }
    logger.info("connection lost, reconnecting...");
    tasks.cancelAll();
    await sleep(reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  adapter.close();
  uiServer?.close();
  if (replaced) process.exit(1);
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((err) => {
    logger.error("client failed", { error: err instanceof Error ? err.message : String(err) });
    process.exit(1);
  });
}
