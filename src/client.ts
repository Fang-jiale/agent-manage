import os from "node:os";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import { readFileSync, existsSync, promises as fsp } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { WebSocket } from "ws";
import * as proto from "./protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, setLogLevel, logger } from "./util.ts";
import { HTTPAdapter } from "./adapters/http.ts";
import { StdioAdapter } from "./adapters/stdio.ts";
import { WSAdapter } from "./adapters/ws.ts";
import { extractTarGz } from "./tar.ts";
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
  productsDir: string; // 产品安装根目录（内部保持 <brand>/<version>/ 结构）
  logLevel: string;
  taskTimeoutMs: number;
}

// connector 配置文件：配对成功后写入，之后零参数启动
interface ConnectorFileConfig {
  gateway?: string;
  connector_id?: string;
  key?: string;
  overrides?: Record<string, LaunchOverride>;
  products_dir?: string;
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
    { name: "products-dir", type: "string" as const, default: envString("AGENT_MANAGE_PRODUCTS_DIR", "") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 7_200_000)) },
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
    taskTimeoutMs = parseDurationMs(values["task-timeout"]) ?? 7_200_000;
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
    productsDir: values["products-dir"] || fileCfg.products_dir || "",
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
    ...(cfg.productsDir ? { products_dir: cfg.productsDir } : {}),
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
        // C1 两级作用域：透传 session_id（存在时表示该 session/workdir 的会话级快照）
        session_id: params.session_id,
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

  const controller = tasks.add(params.task_id);
  try {
    // workdir 在 connector 本机解析：路径不存在时立刻报错并终结任务，
    // 否则任务交给 agent 后无声失败，前端会永远停在"等待回复"
    const workdir = (params.metadata?.workdir as string | undefined) ?? "";
    if (workdir !== "") {
      let bad: string | null = null;
      try {
        if (!(await fsp.stat(workdir)).isDirectory()) bad = "不是目录";
      } catch {
        bad = "不存在";
      }
      if (bad) {
        sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
          proto.PROGRESS_KIND_END,
          { type: proto.CHUNK_TYPE_TEXT, error: `工作目录${bad}：${workdir}`, done: true });
        return;
      }
    }

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
      // adapter.send 内含等待本地 Agent 应答（显式 ack / 错误 / 隐式首 chunk）
      chunks = await adapter.send(req, controller.signal);
    } catch (err) {
      if (controller.signal.aborted && controller.signal.reason === "timeout") {
        sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
          proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: "timeout", done: true });
        return;
      }
      if (controller.signal.aborted) return;
      // 本地 Agent 拒绝（code/message 透传）或连接失败：
      // - 带请求 id（1:1 路径）→ JSON-RPC 错误响应回网关，浏览器 task.create 直接收到
      // - 空 id（群聊 fan-out 无响应路由）→ 发终态 error chunk，进度路径会清理任务
      const code = (err as { code?: number }).code ?? proto.ERR_INTERNAL_ERROR;
      const message = err instanceof Error ? err.message : String(err);
      if (msg.id) {
        sendError(ws, msg.id, code, message);
      } else {
        sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
          proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: message, done: true });
      }
      return;
    }

    // 本地应答成功后才告知网关 accepted——被拒绝的任务从未“被接受”
    safeSend(ws, proto.newResponse(msg.id ?? "", {
      status: "accepted",
      task_id: params.task_id,
    } satisfies proto.TaskAcceptResult));
    sendStatus(ws, agentID, proto.AGENT_STATUS_BUSY, params.task_id);

    // 跨 task.respond 轮次持续消费同一 queue。confirm_required 不再提前关 queue，
    // 后续 task.respond 触发的 chunk 流回这里继续转发给网关。
    let sawDone = false;
    for await (const chunk of chunks) {
      if (chunk.done) sawDone = true;
      const kind = chunk.done ? proto.PROGRESS_KIND_END : proto.PROGRESS_KIND_REPORT;
      sendProgress(ws, agentID, chunk.task_id ?? params.task_id, chunk.session_id ?? params.session_id,
        chunk.context_id ?? params.context_id, kind, chunk);
    }
    // 流结束却没收到任何 done:true（agent 进程崩溃/秒退时 onExit 只是静默关 queue）：
    // 必须补一个错误终点，否则前端永远停在"等待回复"
    if (!sawDone) {
      const reason = controller.signal.aborted && controller.signal.reason === "timeout"
        ? "timeout"
        : "agent 连接中断，未收到结果";
      sendProgress(ws, agentID, params.task_id, params.session_id, params.context_id,
        proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: reason, done: true });
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
    const code = (err as { code?: number }).code;
    if (code === -32000) {
      // confirm 已撤销/已回复的正常竞态（local-agent-interface §6.2.2），静默
      logger.debug("respond race rejected", { task_id: params.task_id, error: String(err) });
      return;
    }
    if (code !== undefined && code !== -32001 && code !== -32003) {
      // agent 显式拒绝（如 -32602）：记录但不终结任务，流侧状态才是真相
      logger.warn("respond rejected by agent", { task_id: params.task_id, code, error: String(err) });
      return;
    }
    // 连接级失败/超时：维持原路径，以终态 error chunk 收尾
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

// agentID：网关认可的实例 ID，随 lifecycle.initialize 的 agentInfo 下发（C3），
// 供 shim 做群管理者受信判断；HTTP 连接方式无 initialize 协商，不适用。
async function createAdapter(connType: string, target: string, agentID?: string): Promise<LocalAgentAdapter> {
  switch (connType) {
    case "stdio":
      return StdioAdapter.create(target, [], agentID);
    case "ws":
      return WSAdapter.create(target, agentID);
    default:
      return HTTPAdapter.create(target);
  }
}

// ---- 本地管理页共享状态：UI HTTP 服务与 connector 主循环经它交互 ----

interface HostState {
  state: "starting" | "running" | "failed" | "stopped";
  error?: string; // failed 时的启动/退出原因
  at: number; // 最近一次状态变迁时间
  retry_at?: number; // 下次自动重试时间（epoch ms）
}

interface LocalUIState {
  connected: boolean;
  lastSync: proto.ConnectorSyncAgent[]; // 最近一次全量目标集
  hosted: Map<string, HostedAgent> | undefined;
  hostStates: Map<string, HostState>; // agent_id → 宿主状态（启动失败原因展示用）
  agentVersions: Map<string, string>; // agent_id → 运行时自报版本（register 捕获，外部安装探测用）
  rpc: (method: string, params: object) => Promise<proto.Message>;
  applyOverride: (agentID: string, override: LaunchOverride | "") => Promise<void>; // 空串 = 清除覆盖
  dropOverride: (agentID: string) => Promise<void>;
  retry?: (agentID: string) => Promise<void>; // 手动重试拉起（connector 模式才有）
  stop?: (agentID: string) => Promise<void>; // 停实例（原地更新前停进程用）
  pairAndConnect?: (gateway: string, code: string, connectorID?: string) => Promise<void>;
}

function newLocalUIState(): LocalUIState {
  return {
    connected: false,
    lastSync: [],
    hosted: undefined,
    hostStates: new Map(),
    agentVersions: new Map(),
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

  // ---- 宿主状态跟踪 + 失败自动重试（5s 起指数退避，封顶 60s；手动重试清零） ----

  const retryTimers = new Map<string, NodeJS.Timeout>();
  const retryAttempts = new Map<string, number>();

  function setHostState(agentID: string, state: HostState["state"], error?: string): void {
    ui.hostStates.set(agentID, { state, error, at: Date.now() });
  }

  function cancelRetry(agentID: string): void {
    const t = retryTimers.get(agentID);
    if (t) {
      clearTimeout(t);
      retryTimers.delete(agentID);
    }
    retryAttempts.delete(agentID);
  }

  function scheduleRetry(a: proto.ConnectorSyncAgent, reason: string): void {
    if (!ui.lastSync.some((x) => x.agent_id === a.agent_id)) return; // 已被 sync 移除，不重试
    cancelRetry(a.agent_id);
    const attempt = (retryAttempts.get(a.agent_id) ?? 0) + 1;
    retryAttempts.set(a.agent_id, attempt);
    const delay = Math.min(5000 * 2 ** Math.min(attempt - 1, 4), 60_000);
    const hs = ui.hostStates.get(a.agent_id);
    if (hs) hs.retry_at = Date.now() + delay;
    const timer = setTimeout(() => {
      retryTimers.delete(a.agent_id);
      const cur = ui.lastSync.find((x) => x.agent_id === a.agent_id);
      if (cur) void ensureAgent(cur).catch(() => { /* 失败已记录状态并排下次重试 */ });
    }, delay);
    timer.unref();
    retryTimers.set(a.agent_id, timer);
    logger.warn("agent host failed, retry scheduled", { agent_id: a.agent_id, error: reason, retry_in_ms: delay, attempt });
  }

  async function ensureAgent(a: proto.ConnectorSyncAgent): Promise<void> {
    const { connType, target } = resolveLaunch(cfg, a);
    // web/app 是"产品"型品牌（打开网页/独立应用），不是可托管的服务——不拉进程、不进重试循环
    if (connType === "web" || connType === "app") {
      setHostState(a.agent_id, "stopped", "产品型品牌（" + connType + "），用「打开」启动，不由此处托管");
      return;
    }
    let h = hosted.get(a.agent_id);
    if (h && (h.connType !== connType || h.target !== target)) {
      // 连接方式/目标变更：停掉旧本地服务，按新配置重建
      logger.info("agent launch changed, rebuilding", { agent_id: a.agent_id });
      await dropAgent(a.agent_id);
      h = undefined;
    }
    if (!h) {
      setHostState(a.agent_id, "starting");
      let adapter: LocalAgentAdapter;
      try {
        adapter = await createAdapter(connType, target, a.agent_id);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        setHostState(a.agent_id, "failed", msg);
        scheduleRetry(a, msg);
        throw err;
      }
      cancelRetry(a.agent_id);
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
            // register 虽然不透传（身份以 sync 为准），但版本号是运行时真相——
            // 外部安装的产品靠它探测版本，不猜目录名
            if (ev.method === proto.METHOD_LIFECYCLE_REGISTER) {
              const v = (ev.params as { version?: string } | undefined | null)?.version;
              if (v) ui.agentVersions.set(a.agent_id, v);
              continue;
            }
            const m = bridgeAgentEvent(currentWS, a.agent_id, adapter, ev, pendingInvokes);
            if (m && currentWS) safeSend(currentWS, m);
          }
          // 流结束 = 子进程退出 / 服务连接断开。实例没被主动 drop（还挂在 hosted）
          // 就是意外死亡：改失败态、通知网关下线、安排重建
          if (hosted.get(a.agent_id) === h) {
            const msg = connType === "stdio" ? "本地子进程已退出" : "本地服务连接已断开";
            logger.error("agent exited unexpectedly", { agent_id: a.agent_id, detail: msg });
            hosted.delete(a.agent_id);
            ui.agentVersions.delete(a.agent_id);
            h.tasks.cancelAll();
            setHostState(a.agent_id, "failed", msg);
            if (currentWS) sendStatus(currentWS, a.agent_id, proto.AGENT_STATUS_OFFLINE);
            scheduleRetry(a, msg);
          }
        })();
      }
      setHostState(a.agent_id, "running");
      logger.info("agent hosted", { agent_id: a.agent_id, brand_id: a.brand_id, launch: target });
    }
    registerToGateway(a.agent_id, h);
  }

  async function dropAgent(agentID: string): Promise<void> {
    cancelRetry(agentID);
    ui.hostStates.delete(agentID);
    ui.agentVersions.delete(agentID);
    const h = hosted.get(agentID);
    if (!h) return;
    hosted.delete(agentID);
    h.tasks.cancelAll();
    h.adapter.close();
    logger.info("agent dropped", { agent_id: agentID });
  }

  ui.retry = async (agentID: string): Promise<void> => {
    const a = ui.lastSync.find((x) => x.agent_id === agentID);
    if (!a) throw new Error("agent 不在当前托管目标集中");
    await dropAgent(agentID); // 清掉残骸与重试计时，退避计数归零后全新拉起
    await ensureAgent(a);
  };
  ui.stop = async (agentID: string): Promise<void> => {
    await dropAgent(agentID);
  };

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
        case proto.METHOD_PRODUCT_PUSH: {
          // 管理端远程推送：本机装过/在跑才动作，否则忽略。纳管安装走升级+实例重指向；
          // 外部安装走原地更新（探测记忆的路径）
          const p = (msg.params ?? {}) as { brand?: string; version?: string };
          const brand = String(p.brand ?? "");
          const version = String(p.version ?? "");
          if (!brand || !version) break;
          void (async () => {
            const installed = listInstalledProducts().some(x => x.brand === brand);
            const externals = loadExternals();
            logger.info("product push received", { brand, version, installed });
            if (installed) {
              const { buf, entry } = await fetchRemotePackage(cfg, brand, version);
              const r = installProduct(buf, brand + "-" + version + ".tar.gz",
                typeof entry.sha256 === "string" ? entry.sha256 : null,
                entry.manifest as Record<string, unknown> | undefined);
              // 实例重指向（与 install-remote 同款）
              const manifest = entry.manifest as unknown as ProductManifest;
              const ov = overrideTargetFor(manifest, r.install_dir);
              if (ov) {
                try {
                  const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {});
                  const brands = bl.error ? [] : ((bl.result as { brands?: proto.BrandInfo[] }).brands || []);
                  const match = brands.find(b => b.name === manifest.brand || b.name === (manifest.name || manifest.brand));
                  if (match) {
                    for (const a of ui.lastSync) {
                      if (a.brand_id === match.id) await ui.applyOverride(a.agent_id, ov);
                    }
                  }
                } catch { /* 重指向失败不影响安装 */ }
              }
              logger.info("product push: managed upgrade done", { brand, version, dir: r.install_dir });
              return;
            }
            // 外部安装：原地更新（记忆路径；没有记忆就记日志等下次探测）
            const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {}).catch(() => null);
            const brands = bl && !bl.error ? ((bl.result as { brands?: proto.BrandInfo[] }).brands || []) : [];
            const match = brands.find(b => b.name === brand);
            const ext = (match && externals[match.id]) || null;
            if (!ext) {
              logger.info("product push ignored: not installed here", { brand });
              return;
            }
            const instanceIDs: string[] = [];
            if (match) {
              for (const a of ui.lastSync) {
                if (a.brand_id === match.id) instanceIDs.push(a.agent_id);
              }
            }
            for (const id of instanceIDs) { if (ui.stop) await ui.stop(id); }
            if (localRuns.has(brand)) { try { stopLocalRun(brand); } catch { /* 未在跑 */ } }
            const { buf, entry } = await fetchRemotePackage(cfg, brand, version);
            const r = updateInPlace(ext.path, buf, typeof entry.sha256 === "string" ? entry.sha256 : null);
            for (const id of instanceIDs) { if (ui.retry) await ui.retry(id).catch(() => {}); }
            logger.info("product push: in-place update done", { brand, version, path: ext.path, backup: r.backup });
          })().catch((e) => logger.error("product push failed", { brand, version, error: String(e) }));
          break;
        }
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

function readRawBody(req: http.IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
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

// ---- 产品安装引擎：tar.gz 安装包（内含 manifest.json）→ ~/.agent-manage/products/<brand>/<version>/ ----
// 目录约定（详见 docs/installer-spec.md）：
//   products/<brand>/<version>/   侧车式版本目录，升级装新目录不覆盖
//   products/<brand>/current      指针文件，内容为当前版本号（原子切换）
//   products/<brand>/manifest.json 当前版本 manifest 副本（列表展示用）

interface ProductManifest {
  format: number;
  brand: string; // 品牌 slug，同时作目录名
  version: string; // semver
  kind: string; // stdio | http | ws | web | app
  name?: string;
  description?: string;
  launch_cmd?: string | null; // kind=stdio：可含 {{install_dir}} 占位
  endpoint?: string | null; // kind=http/ws/web/app：地址或应用路径
  capabilities?: proto.Capability[];
}

interface InstalledProduct {
  brand: string;
  version: string; // current 指向的版本
  versions: string[];
  install_dir: string;
  manifest: ProductManifest | null;
}

// 产品安装根目录可自定义（磁盘规划）；<brand>/<version>/ 内部结构是升级/回滚机制的一部分，不可打散
let productsRootOverride: string | null = null;
function setProductsRoot(dir: string): void {
  productsRootOverride = dir && dir.trim() ? path.resolve(dir.trim()) : null;
  if (productsRootOverride) fs.mkdirSync(productsRootOverride, { recursive: true });
}
function productsRoot(): string {
  return productsRootOverride ?? path.join(os.homedir(), ".agent-manage", "products");
}

function validBrandSlug(s: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(s);
}
function validVersion(s: string): boolean {
  return /^\d+\.\d+\.\d+(-[0-9A-Za-z.+-]+)?$/.test(s);
}

// 语义化版本三段比较（忽略 prerelease 细节，够安装器防降级用）
function versionTuple(v: string): [number, number, number] {
  const m = /^(\d+)\.(\d+)\.(\d+)/.exec(v);
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : [0, 0, 0];
}
function versionLt(a: string, b: string): boolean {
  const [a1, a2, a3] = versionTuple(a);
  const [b1, b2, b3] = versionTuple(b);
  return a1 !== b1 ? a1 < b1 : a2 !== b2 ? a2 < b2 : a3 < b3;
}

function readPointer(brandRoot: string): string | null {
  try {
    const v = fs.readFileSync(path.join(brandRoot, "current"), "utf8").trim();
    return validVersion(v) ? v : null;
  } catch {
    return null;
  }
}

// 只保留 current + 最新 2 个版本目录，其余清理
function pruneVersions(brandRoot: string, keepCurrent: string): void {
  let versions: string[] = [];
  try {
    versions = fs.readdirSync(brandRoot, { withFileTypes: true })
      .filter(e => e.isDirectory() && validVersion(e.name)).map(e => e.name);
  } catch { return; }
  versions.sort((a, b) => (versionLt(a, b) ? -1 : versionLt(b, a) ? 1 : 0));
  const keep = new Set([keepCurrent, ...versions.slice(-2)]);
  for (const v of versions) {
    if (!keep.has(v)) {
      fs.rmSync(path.join(brandRoot, v), { recursive: true, force: true });
      logger.info("pruned old product version", { brand: path.basename(brandRoot), version: v });
    }
  }
}

function listInstalledProducts(): InstalledProduct[] {
  const root = productsRoot();
  let brands: fs.Dirent[] = [];
  try {
    brands = fs.readdirSync(root, { withFileTypes: true }).filter(e => e.isDirectory() && !e.name.startsWith("."));
  } catch {
    return []; // 目录不存在 = 没装过
  }
  return brands.map(e => {
    const brandRoot = path.join(root, e.name);
    const current = readPointer(brandRoot);
    let versions: string[] = [];
    try {
      versions = fs.readdirSync(brandRoot, { withFileTypes: true })
        .filter(d => d.isDirectory() && validVersion(d.name)).map(d => d.name);
    } catch { /* 空品牌目录 */ }
    let manifest: ProductManifest | null = null;
    try {
      manifest = JSON.parse(fs.readFileSync(path.join(brandRoot, "manifest.json"), "utf8")) as ProductManifest;
    } catch { /* 无副本 */ }
    return {
      brand: e.name,
      version: current ?? versions[versions.length - 1] ?? "",
      versions,
      install_dir: current ? path.join(brandRoot, current) : "",
      manifest,
    };
  });
}

// 安装：staging 解包 → 校验 manifest → 版本目录落位 → 指针切换 → 清旧版本
function installProduct(buf: Buffer, filename: string, sha256?: string | null,
  manifestOverride?: Record<string, unknown>): {
  brand: string; version: string; install_dir: string; files: number; upgraded: boolean;
} {
  if (sha256) {
    const actual = crypto.createHash("sha256").update(buf).digest("hex");
    if (actual !== sha256.toLowerCase()) throw new Error("校验和不匹配：包可能损坏或被替换");
  }
  const root = productsRoot();
  fs.mkdirSync(root, { recursive: true });
  const staging = fs.mkdtempSync(path.join(root, ".staging-"));
  let manifestDir = staging;
  let manifestPath = path.join(staging, "manifest.json");
  try {
    const files = extractTarGz(buf, staging);
    // manifest 允许在包根，或唯一一级子目录内（tar 打包常见 ./dir/ 前缀）
    if (!existsSync(manifestPath)) {
      const dirs = fs.readdirSync(staging, { withFileTypes: true })
        .filter(e => e.isDirectory() && !e.name.startsWith("."));
      if (dirs.length === 1) {
        manifestDir = path.join(staging, dirs[0].name);
        manifestPath = path.join(manifestDir, "manifest.json");
      }
    }
    if (!existsSync(manifestPath) && !manifestOverride) throw new Error("安装包内缺少 manifest.json（" + filename + "）");
    // 远程安装时服务端 manifest 为准（管理端可编辑），包内版本仅作回退
    const manifest = (manifestOverride
      ? JSON.parse(JSON.stringify(manifestOverride)) as ProductManifest
      : JSON.parse(fs.readFileSync(manifestPath, "utf8")) as ProductManifest);
    if (manifest.format !== 1) throw new Error("不支持的 manifest format: " + String(manifest.format));
    if (!validBrandSlug(manifest.brand)) throw new Error("manifest.brand 非法: " + String(manifest.brand));
    if (!validVersion(manifest.version)) throw new Error("manifest.version 需要 semver（如 1.0.0）: " + String(manifest.version));
    if (!["stdio", "http", "ws", "web", "app"].includes(manifest.kind)) {
      throw new Error("manifest.kind 非法: " + String(manifest.kind));
    }
    const brandRoot = path.join(root, manifest.brand);
    const dest = path.join(brandRoot, manifest.version);
    if (existsSync(dest)) throw new Error("版本 " + manifest.version + " 已存在（同版本重装请先卸载）");
    const current = readPointer(brandRoot);
    if (current && versionLt(manifest.version, current)) {
      throw new Error("不允许降级安装：当前 " + current + "，包为 " + manifest.version);
    }
    fs.mkdirSync(brandRoot, { recursive: true });
    fs.renameSync(manifestDir, dest);
    fs.rmSync(staging, { recursive: true, force: true });
    if (manifestOverride && !existsSync(path.join(dest, "manifest.json"))) {
      fs.writeFileSync(path.join(dest, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
    }
    // 指针 + manifest 副本：先落副本再切指针（失败时指针仍指旧版本）
    fs.writeFileSync(path.join(brandRoot, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
    fs.writeFileSync(path.join(brandRoot, "current"), manifest.version, "utf8");
    pruneVersions(brandRoot, manifest.version);
    logger.info("product installed", { brand: manifest.brand, version: manifest.version, files, upgraded: current !== null });
    return { brand: manifest.brand, version: manifest.version, install_dir: dest, files, upgraded: current !== null };
  } catch (e) {
    fs.rmSync(staging, { recursive: true, force: true });
    throw e;
  }
}

function uninstallProduct(brand: string): void {
  if (!validBrandSlug(brand)) throw new Error("非法品牌名");
  const brandRoot = path.join(productsRoot(), brand);
  if (!existsSync(brandRoot)) throw new Error("未安装: " + brand);
  fs.rmSync(brandRoot, { recursive: true, force: true });
  logger.info("product uninstalled", { brand });
}

// cfg.gateway(ws(s)://host:port/ws/agent)→HTTP 基址（产品目录/下载走 HTTP）
function gatewayHttpBase(gateway: string): string {
  const m = /^(wss?):\/\/([^/?#]+)/.exec(gateway.trim());
  if (!m) throw new Error("网关地址无法解析: " + gateway);
  return (m[1] === "wss" ? "https" : "http") + "://" + m[2];
}

// manifest → 本机 override target（{{install_dir}} 解析）；web/app 返回 null（不托管）
function overrideTargetFor(manifest: ProductManifest, installDir: string): { conn_type: string; target: string } | null {
  if (manifest.kind === "stdio") {
    if (!manifest.launch_cmd) return null;
    return { conn_type: "stdio", target: manifest.launch_cmd.replace(/\{\{install_dir\}\}/g, installDir) };
  }
  if (manifest.kind === "http" || manifest.kind === "ws") {
    if (!manifest.endpoint) return null;
    return { conn_type: manifest.kind, target: manifest.endpoint };
  }
  return null;
}

// 跨平台拉起：可执行路径直接 spawn，失败回退系统 opener
function launchEndpoint(target: string): { launched: boolean; error?: string } {
  try {
    const child = spawn(target, [], { detached: true, stdio: "ignore", shell: process.platform === "win32" });
    child.unref();
    return { launched: true };
  } catch (e) {
    const opener = process.platform === "darwin" ? "open"
      : process.platform === "win32" ? "cmd" : "xdg-open";
    const args = process.platform === "win32" ? ["/c", "start", "", target] : [target];
    try {
      const child = spawn(opener, args, { detached: true, stdio: "ignore" });
      child.unref();
      return { launched: true };
    } catch (e2) {
      return { launched: false, error: (e instanceof Error ? e.message : String(e)) + "; " + (e2 instanceof Error ? e2.message : String(e2)) };
    }
  }
}

// ---- 原地更新：对任意已探测路径下发新版本（备份→换目录→重启→可回滚） ----

// 安全校验：拒绝系统目录与 Git 仓库（原地覆盖会毁版本库），要求可写
function inPlaceBlockReason(dir: string): string | null {
  const blocked = [/^\/System/i, /^\/usr/i, /^\/bin/i, /^\/sbin/i, /^\/etc/i, /^\/Applications/i, /^\/Library/i, /^C:\\Windows/i];
  for (const re of blocked) if (re.test(dir)) return "系统/应用目录不允许原地更新";
  let cur = path.resolve(dir);
  for (let i = 0; i < 24; i++) {
    if (existsSync(path.join(cur, ".git"))) return "路径位于 Git 仓库内（" + cur + "），原地更新会破坏版本库——请改用纳管安装";
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  try { fs.accessSync(dir, fs.constants.W_OK); } catch { return "目录不可写"; }
  return null;
}

function updateInPlace(destDir: string, buf: Buffer, sha256?: string | null): { files: number; backup: string } {
  if (!existsSync(destDir) || !fs.statSync(destDir).isDirectory()) throw new Error("目标路径不存在或不是目录: " + destDir);
  const block = inPlaceBlockReason(destDir);
  if (block) throw new Error(block);
  if (sha256) {
    const actual = crypto.createHash("sha256").update(buf).digest("hex");
    if (actual !== sha256.toLowerCase()) throw new Error("校验和不匹配：包可能损坏或被替换");
  }
  const parent = path.dirname(destDir);
  const staging = fs.mkdtempSync(path.join(parent, ".update-staging-"));
  const backup = destDir + ".bak-" + Date.now();
  try {
    const files = extractTarGz(buf, staging);
    // 备份旧的 → 新内容顶上（同文件系统 rename，原子且可回滚）
    fs.renameSync(destDir, backup);
    fs.renameSync(staging, destDir);
    // 只留最近一份备份
    let baks = fs.readdirSync(parent).filter(n => n.startsWith(path.basename(destDir) + ".bak-"));
    baks.sort();
    for (const old of baks.slice(0, -1)) fs.rmSync(path.join(parent, old), { recursive: true, force: true });
    logger.info("product updated in place", { dir: destDir, backup, files });
    return { files, backup };
  } catch (e) {
    if (existsSync(backup) && !existsSync(destDir)) {
      try { fs.renameSync(backup, destDir); } catch { /* 回滚失败只能人工介入 */ }
    }
    fs.rmSync(staging, { recursive: true, force: true });
    throw e;
  }
}

// 从网关取包：目录条目 + 包体（install-remote 与原地更新共用语义）
async function fetchRemotePackage(cfg: ClientConfig, brand: string, version: string): Promise<{
  buf: Buffer; entry: Record<string, unknown>;
}> {
  const base = gatewayHttpBase(cfg.gateway);
  const cr = await fetch(base + "/products/catalog", { signal: AbortSignal.timeout(8000) });
  const catalog = cr.ok ? ((await cr.json()) as { products?: Array<Record<string, unknown>> }).products || [] : [];
  const entry = catalog.find(x => x.brand === brand && x.version === version);
  if (!entry) throw new Error("网关目录里没有 " + brand + " " + version);
  const dr = await fetch(base + "/products/" + brand + "/" + version + "/download", { signal: AbortSignal.timeout(300_000) });
  if (!dr.ok) throw new Error("下载失败（HTTP " + dr.status + "）");
  return { buf: Buffer.from(await dr.arrayBuffer()), entry };
}

// ---- 本地运行（不接网关）：把已装产品按 manifest 命令拉起，日志落文件，可停 ----
const localRuns = new Map<string, { child: ReturnType<typeof spawn>; logPath: string; startedAt: number }>();

function localRunOf(brand: string): { running: boolean; log_path?: string; started_at?: number } {
  const r = localRuns.get(brand);
  if (!r) return { running: false };
  return { running: !r.child.killed && r.child.exitCode === null, log_path: r.logPath, started_at: r.startedAt };
}

function startLocalRun(brand: string): { running: boolean; log_path: string; error?: string } {
  const products = listInstalledProducts().filter(x => x.brand === brand);
  const prod = products[0];
  if (!prod || !prod.manifest || !prod.install_dir) throw new Error("本机未安装该产品: " + brand);
  if (localRuns.has(brand)) {
    const cur = localRuns.get(brand);
    if (cur && cur.child.exitCode === null) return { running: true, log_path: cur.logPath };
    localRuns.delete(brand);
  }
  const ov = overrideTargetFor(prod.manifest, prod.install_dir);
  const logPath = path.join(productsRoot(), brand, "local-run.log");
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  const manifest = prod.manifest;
  if (manifest.kind === "web" || manifest.kind === "app") {
    // 产品型：web/app 不需要进程托管，直接打开
    const target = manifest.endpoint || "";
    if (!target) throw new Error("manifest 未配置 endpoint");
    if (manifest.kind === "web") throw new Error("web 型产品请在目录里点「打开」");
    const r = launchEndpoint(target);
    if (!r.launched) throw new Error("启动失败: " + r.error);
    return { running: false, log_path: logPath };
  }
  if (!ov) throw new Error("manifest 缺少 launch_cmd / endpoint，无法本地启动");
  // tokenizeCommand 与 stdio 适配器同款语义：命令字符串拆 argv
  const log = fs.openSync(logPath, "a");
  fs.writeSync(log, "\n===== local run " + new Date().toISOString() + " =====\n");
  const child = spawn(ov.target, [], {
    shell: true,                       // 命令字符串原样执行（与品牌 launch_cmd 语义一致）
    detached: false, stdio: ["ignore", log, log],
    env: { ...process.env },
  });
  child.on("exit", (code, sig) => {
    try { fs.writeSync(log, "\n===== exited code=" + code + " sig=" + sig + " =====\n"); } catch { /* 日志句柄可能已关 */ }
  });
  localRuns.set(brand, { child, logPath, startedAt: Date.now() });
  logger.info("product local run started", { brand, pid: child.pid, log: logPath });
  return { running: true, log_path: logPath };
}

function stopLocalRun(brand: string): void {
  const r = localRuns.get(brand);
  if (!r) throw new Error("该产品没有在本地运行");
  if (r.child.exitCode === null) {
    try { r.child.kill("SIGTERM"); } catch { /* 已退出 */ }
  }
  localRuns.delete(brand);
  logger.info("product local run stopped", { brand });
}

// ---- 外部安装探测：从实例生效命令解析安装根路径，记忆到 externals.json ----
// 实例停掉后仍可见可更新（记忆最后一次看到的路径/版本）；探测不依赖文件系统扫描。

interface ExternalInstall { path: string; version: string | null; last_seen: number; }

function externalsPath(): string {
  return path.join(productsRoot(), ".externals.json");
}
function loadExternals(): Record<string, ExternalInstall> {
  try {
    return JSON.parse(fs.readFileSync(externalsPath(), "utf8")) as Record<string, ExternalInstall>;
  } catch {
    return {};
  }
}
function saveExternals(map: Record<string, ExternalInstall>): void {
  fs.mkdirSync(productsRoot(), { recursive: true });
  fs.writeFileSync(externalsPath(), JSON.stringify(map, null, 2), "utf8");
}

// 从启动命令解析安装根：跳过解释器后取第一个磁盘上存在的文件型 token 的所在目录。
// node /abs/path/bin.js --config x.yml → /abs/path；相对路径按 connector cwd 解析；
// 纯 URL（http/ws 型）返回 null
const INTERPRETERS = new Set(["node", "node.exe", "python", "python3", "python.exe", "ruby", "deno", "bun", "env"]);
function installPathOf(target: string): string | null {
  const tokens = target.split(/\s+/).filter(Boolean);
  for (const t of tokens) {
    if (!t.includes("/") && !t.includes("\\")) continue;
    if (/^[a-z]+:\/\//i.test(t)) continue; // URL
    if (/^-/.test(t)) continue; // flag
    const probe = t.replace(/^['"]|['"]$/g, "").replace(/\/$/, "");
    if (INTERPRETERS.has(path.basename(probe))) continue; // 解释器不是产物
    for (const cand of [path.resolve(process.cwd(), probe), probe]) {
      try {
        if (fs.existsSync(cand) && fs.statSync(cand).isFile()) return path.dirname(path.resolve(cand));
      } catch { /* 不可访问的 token 跳过 */ }
    }
  }
  return null;
}

// 探测并记忆：以运行实例为准刷新 externals 映射（brand → path/version）
function refreshExternals(cfg: ClientConfig, ui: LocalUIState): Record<string, ExternalInstall> {
  const map = loadExternals();
  const managedRoot = productsRoot() + path.sep;
  let dirty = false;
  for (const a of ui.lastSync) {
    const { target } = resolveLaunch(cfg, a);
    if (!target) continue;
    const root = installPathOf(target);
    if (!root) continue;
    if ((root + path.sep).startsWith(managedRoot)) {
      // 纳管安装走 products 目录：清掉该品牌可能残留的旧外部记忆
      if (map[a.brand_id]) { delete map[a.brand_id]; dirty = true; }
      continue;
    }
    const ver = ui.agentVersions.get(a.agent_id) ?? null;
    const prev = map[a.brand_id];
    if (!prev || prev.path !== root || prev.version !== ver) {
      // brand_id 做键（品牌改名也不串），展示时再换回品牌名
      map[a.brand_id] = { path: root, version: ver, last_seen: Date.now() };
      dirty = true;
    } else {
      prev.last_seen = Date.now();
      dirty = true;
    }
  }
  if (dirty) saveExternals(map);
  return map;
}

// 统一产品目录：网关品牌 × 远程产品包 × 本机安装 × 本地运行状态 合成一张表
async function buildProductCatalog(cfg: ClientConfig, ui: LocalUIState): Promise<unknown[]> {
  const base = gatewayHttpBase(cfg.gateway);
  // 远程包目录（网关不可达时降级为空）
  let remote: Array<Record<string, unknown>> = [];
  try {
    const r = await fetch(base + "/products/catalog", { signal: AbortSignal.timeout(8000) });
    if (r.ok) remote = ((await r.json()) as { products?: Array<Record<string, unknown>> }).products || [];
  } catch { /* 离线：只展示本机 */ }
  // 品牌身份（连着网关才有；logo 相对路径补成网关绝对地址）
  let brands: proto.BrandInfo[] = [];
  try {
    const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {});
    if (!bl.error) brands = ((bl.result as { brands?: proto.BrandInfo[] }).brands || []);
  } catch { /* 未连接 */ }
  const installed = listInstalledProducts();
  const externals = refreshExternals(cfg, ui);
  const externalOf = (brandID: string | null, brandName: string): ExternalInstall | null => {
    if (brandID && externals[brandID]) return externals[brandID];
    return Object.values(externals).find(e => e.path.includes(brandName)) ?? null;
  };
  const rows: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  for (const b of brands) {
    if (b.disabled) continue;
    seen.add(b.name);
    const inst = installed.find(p => p.brand === b.name);
    const pkg = remote.filter(e => e.brand === b.name).sort((x, y) => String(y.updated_at).localeCompare(String(x.updated_at)))[0];
    // 实例判断细分：有实例，且其生效命令落在 products/<brand>/ 内 = 接的是产品目录这份；
    // 有实例但命令在外部（仓库目录/手动指定）= 外部命令接入，不受安装器管理
    const instances = ui.lastSync.filter(a => a.brand_id === b.id);
    const runtimeVersion = instances.map(a => ui.agentVersions.get(a.agent_id)).find(Boolean) ?? null;
    const instDirPrefix = inst?.install_dir || path.join(productsRoot(), b.name);
    const fromInstall = instances.some(a => {
      const { target } = resolveLaunch(cfg, a);
      return target.includes(instDirPrefix);
    });
    rows.push({
      brand: b.name,
      brand_id: b.id,
      name: inst?.manifest?.name || (pkg?.manifest as Record<string, unknown> | undefined)?.name || b.name,
      description: b.description || (pkg?.manifest as Record<string, unknown> | undefined)?.description || "",
      kind: b.conn_type || (pkg?.manifest as Record<string, unknown> | undefined)?.kind || "stdio",
      logo_url: b.logo_url ? (String(b.logo_url).startsWith("http") ? b.logo_url : base + b.logo_url) : null,
      installed_version: inst?.version || null,
      remote_version: pkg ? String(pkg.version) : null,
      remote_size: pkg ? pkg.size : null,
      has_instance: instances.length > 0,
      instance_from_install: fromInstall,
      runtime_version: runtimeVersion,
      external: fromInstall ? null : externalOf(b.id, b.name),
      local_run: localRunOf(b.name),
    });
  }
  // 只在本机出现（离线装了、或网关没这个品牌）的产品补在后面
  for (const p of installed) {
    if (seen.has(p.brand)) continue;
    rows.push({
      brand: p.brand,
      brand_id: null,
      name: p.manifest?.name || p.brand,
      description: p.manifest?.description || "",
      kind: p.manifest?.kind || "stdio",
      logo_url: null,
      installed_version: p.version || null,
      remote_version: null,
      remote_size: null,
      has_instance: false,
      instance_from_install: false,
      local_run: localRunOf(p.brand),
    });
  }
  return rows;
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
      const hs = ui.hostStates.get(a.agent_id);
      return {
        agent_id: a.agent_id,
        brand_id: a.brand_id,
        name: a.name,
        conn_type: connType,
        launch_cmd: target,
        launch_cmd_source: cfg.overrides[a.agent_id] !== undefined
          ? "override"
          : ((a.launch_cmd ?? a.endpoint) ? "brand" : "local"),
        state: hs?.state ?? (ui.hosted?.has(a.agent_id) ? "running" : "stopped"),
        error: hs?.error ?? null,
        retry_at: hs?.retry_at ?? null,
        version: ui.agentVersions.get(a.agent_id) ?? null,
      };
    });
    sendJSON(res, 200, {
      configured: cfg.deviceKey !== "" || cfg.token !== "",
      connected: ui.connected,
      gateway: cfg.gateway,
      connector_id: cfg.connectorID,
      config_path: cfg.configPath,
      products_dir: productsRoot(),
      agents,
    });
    return;
  }
  if (req.method === "PUT" && p === "/api/settings/products-dir") {
    const body = await readBody(req);
    const dir = String(body.dir ?? "").trim();
    if (dir && !path.isAbsolute(dir)) {
      sendJSON(res, 400, { error: "请填写绝对路径" });
      return;
    }
    cfg.productsDir = dir;
    await saveConfig(cfg);
    setProductsRoot(dir);
    logger.info("products root changed", { dir: productsRoot() });
    sendJSON(res, 200, { products_dir: productsRoot() });
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
  const mr = /^\/api\/agents\/([^/]+)\/retry$/.exec(p);
  if (mr && req.method === "POST") {
    if (!ui.retry) {
      sendJSON(res, 409, { error: "connector 未运行" });
      return;
    }
    try {
      await ui.retry(mr[1]);
      sendJSON(res, 200, { status: "ok" });
    } catch (e) {
      sendJSON(res, 500, { error: e instanceof Error ? e.message : String(e) });
    }
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

  /* ---------- 产品中心：安装 / 列表 / 卸载 / 建实例 / 启动 ---------- */

  if (req.method === "GET" && p === "/api/products") {
    sendJSON(res, 200, { products: listInstalledProducts() });
    return;
  }

  // 统一产品目录：品牌 × 远程包 × 本机安装 × 本地运行 一张表
  if (req.method === "GET" && p === "/api/product-catalog") {
    try {
      sendJSON(res, 200, { gateway: gatewayHttpBase(cfg.gateway), products: await buildProductCatalog(cfg, ui) });
    } catch (e) {
      sendJSON(res, 500, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  const mlr = /^\/api\/products\/([A-Za-z0-9._-]+)\/local-run\/(start|stop)$/.exec(p);
  if (mlr && req.method === "POST") {
    try {
      if (mlr[2] === "start") {
        const r = startLocalRun(decodeURIComponent(mlr[1]));
        sendJSON(res, 200, r);
      } else {
        stopLocalRun(decodeURIComponent(mlr[1]));
        sendJSON(res, 200, { status: "ok" });
      }
    } catch (e) {
      sendJSON(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  // 远程产品目录（网关 data/products；未连网关/网关不可达返回空并标注）
  if (req.method === "GET" && p === "/api/remote-products") {
    try {
      const r = await fetch(gatewayHttpBase(cfg.gateway) + "/products/catalog",
        { signal: AbortSignal.timeout(8000) });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const body = await r.json() as { products?: unknown[] };
      sendJSON(res, 200, { gateway: gatewayHttpBase(cfg.gateway), products: body.products || [] });
    } catch (e) {
      sendJSON(res, 200, { gateway: gatewayHttpBase(cfg.gateway), products: [], error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  // 一键远程安装：网关下载 → 服务端 sha256 校验 → 安装 → 已有实例自动重指向新版本目录
  // 原地更新：对外部安装路径直接下发新版本（探测记忆的路径，或请求体显式指定）
  const muip = /^\/api\/products\/([A-Za-z0-9._-]+)\/((?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?)\/update-in-place$/.exec(p);
  if (muip && req.method === "POST") {
    const brand = muip[1];
    const version = muip[2];
    try {
      const body = await readBody(req);
      let dest = String(body.path ?? "").trim();
      if (!dest) {
        // 未指定路径：用 externals 记忆（brand_id 优先，其次品牌名模糊匹配）
        const externals = loadExternals();
        const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {}).catch(() => null);
        const brands = bl && !bl.error ? ((bl.result as { brands?: proto.BrandInfo[] }).brands || []) : [];
        const match = brands.find(b => b.name === brand);
        const ext = (match && externals[match.id]) || Object.values(externals).find(e => e.path.includes(brand));
        if (!ext) throw new Error("未指定 path，且没有该产品的外部安装记忆（先让实例跑一次即可探测到）");
        dest = ext.path;
      }
      const { buf, entry } = await fetchRemotePackage(cfg, brand, version);
      // 目标目录与该品牌实例解析出的路径对齐校验（防误伤别的目录）
      const manifest = entry.manifest as unknown as ProductManifest;
      // 停掉本品牌相关进程：网关实例 + 本地运行
      const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {}).catch(() => null);
      const brands = bl && !bl.error ? ((bl.result as { brands?: proto.BrandInfo[] }).brands || []) : [];
      const match = brands.find(b => b.name === brand || b.name === (manifest.name || brand));
      const instanceIDs: string[] = [];
      if (match) {
        for (const a of ui.lastSync) {
          if (a.brand_id !== match.id) continue;
          instanceIDs.push(a.agent_id);
          if (ui.stop) await ui.stop(a.agent_id);
        }
      }
      if (localRuns.has(brand)) {
        try { stopLocalRun(brand); } catch { /* 未在跑 */ }
      }
      const r = updateInPlace(dest, buf, typeof entry.sha256 === "string" ? entry.sha256 : null);
      // 重启实例（同路径新内容；启动失败可人工回滚 backup）
      const restarted: string[] = [];
      for (const id of instanceIDs) {
        if (ui.retry) {
          try { await ui.retry(id); restarted.push(id); } catch { /* 重试调度已接管 */ }
        }
      }
      sendJSON(res, 200, { brand, version, path: dest, files: r.files, backup: r.backup, restarted });
    } catch (e) {
      sendJSON(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  const mir = /^\/api\/products\/([A-Za-z0-9._-]+)\/((?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?)\/install-remote$/.exec(p);
  if (mir && req.method === "POST") {
    const brand = mir[1];
    const version = mir[2];
    try {
      const base = gatewayHttpBase(cfg.gateway);
      const cr = await fetch(base + "/products/catalog", { signal: AbortSignal.timeout(8000) });
      const catalog = cr.ok ? ((await cr.json()) as { products?: Array<Record<string, unknown>> }).products || [] : [];
      const entry = catalog.find(x => x.brand === brand && x.version === version);
      if (!entry) throw new Error("网关目录里没有 " + brand + " " + version);
      const dr = await fetch(base + "/products/" + brand + "/" + version + "/download",
        { signal: AbortSignal.timeout(300_000) });
      if (!dr.ok) throw new Error("下载失败（HTTP " + dr.status + "）");
      const buf = Buffer.from(await dr.arrayBuffer());
      const installed = installProduct(buf, brand + "-" + version + ".tar.gz",
        typeof entry.sha256 === "string" ? entry.sha256 : null,
        entry.manifest as Record<string, unknown> | undefined);
      // 同品牌已有实例 → override 自动指到新目录（connector sync 后自动重启到新版本）。
      // 重指向依赖网关 WS，失败不影响安装结果
      const repointed: string[] = [];
      const manifest = entry.manifest as unknown as ProductManifest;
      const ov = overrideTargetFor(manifest, installed.install_dir);
      if (ov) {
        try {
          const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {});
          const brands = bl.error ? [] : ((bl.result as { brands?: proto.BrandInfo[] }).brands || []);
          const match = brands.find(b => b.name === manifest.brand || b.name === (manifest.name || manifest.brand));
          if (match) {
            for (const a of ui.lastSync) {
              if (a.brand_id !== match.id) continue;
              await ui.applyOverride(a.agent_id, ov);
              repointed.push(a.agent_id);
            }
          }
        } catch (e) {
          logger.warn("product installed but repoint skipped", { error: e instanceof Error ? e.message : String(e) });
        }
      }
      sendJSON(res, 200, { ...installed, manifest, repointed });
    } catch (e) {
      sendJSON(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  if (req.method === "POST" && p === "/api/products/install") {
    const filename = url.searchParams.get("filename") || "package.tgz";
    if (!/\.(tar\.gz|tgz)$/i.test(filename)) {
      sendJSON(res, 400, { error: "仅支持 .tar.gz / .tgz 安装包" });
      return;
    }
    const buf = await readRawBody(req);
    try {
      const r = installProduct(buf, filename, url.searchParams.get("sha256"));
      sendJSON(res, 200, r);
    } catch (e) {
      sendJSON(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  const mp = /^\/api\/products\/([^/]+)$/.exec(p);
  if (mp && req.method === "DELETE") {
    try {
      uninstallProduct(decodeURIComponent(mp[1]));
      sendJSON(res, 200, { status: "ok" });
    } catch (e) {
      sendJSON(res, 400, { error: e instanceof Error ? e.message : String(e) });
    }
    return;
  }

  const mip = /^\/api\/products\/([^/]+)\/instantiate$/.exec(p);
  if (mip && req.method === "POST") {
    const brand = decodeURIComponent(mip[1]);
    const products = listInstalledProducts().filter(x => x.brand === brand);
    const prod = products[0];
    if (!prod || !prod.manifest) {
      sendJSON(res, 404, { error: "本机未安装该产品: " + brand });
      return;
    }
    const manifest = prod.manifest;
    // 在网关品牌目录里找同名品牌（品牌=身份；装的产品与品牌同名约定见 installer-spec）
    const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {});
    if (bl.error) {
      sendJSON(res, 502, { error: bl.error.message });
      return;
    }
    const brands = (bl.result as { brands?: proto.BrandInfo[] }).brands || [];
    const match = brands.find(b => b.name === manifest.brand || b.name === (manifest.name || manifest.brand));
    if (!match) {
      sendJSON(res, 409, { error: "网关品牌目录里没有「" + manifest.brand + "」，请先在管理后台创建同名品牌" });
      return;
    }
    const r = await ui.rpc(proto.METHOD_AGENT_ASSIGN, {
      connector_id: cfg.connectorID,
      brand_id: match.id,
      name: manifest.name || undefined,
    } satisfies proto.AgentAssignParams);
    if (r.error) {
      sendJSON(res, 502, { error: r.error.message });
      return;
    }
    const agentID = (r.result as proto.AgentAssignResult).agent_id;
    // stdio 产品：launch_cmd 的 {{install_dir}} 解析为本机版本目录，写进本机覆盖
    if (manifest.kind === "stdio" && manifest.launch_cmd) {
      const target = manifest.launch_cmd.replace(/\{\{install_dir\}\}/g, prod.install_dir);
      await ui.applyOverride(agentID, { conn_type: "stdio", target });
    } else if ((manifest.kind === "http" || manifest.kind === "ws") && manifest.endpoint) {
      await ui.applyOverride(agentID, { conn_type: manifest.kind, target: manifest.endpoint });
    }
    sendJSON(res, 200, { agent_id: agentID, install_dir: prod.install_dir });
    return;
  }

  if (req.method === "POST" && p === "/api/launch") {
    const body = await readBody(req);
    const brandID = String(body.brand_id ?? "");
    if (!brandID) {
      sendJSON(res, 400, { error: "brand_id required" });
      return;
    }
    const bl = await ui.rpc(proto.METHOD_BRAND_LIST, {});
    if (bl.error) {
      sendJSON(res, 502, { error: bl.error.message });
      return;
    }
    const brands = (bl.result as { brands?: proto.BrandInfo[] }).brands || [];
    const b = brands.find(x => x.id === brandID);
    if (!b) {
      sendJSON(res, 404, { error: "品牌不存在" });
      return;
    }
    const target = b.endpoint || "";
    if (b.conn_type === "web") {
      if (!target) {
        sendJSON(res, 400, { error: "该品牌未配置网址" });
        return;
      }
      sendJSON(res, 200, { kind: "web", url: target }); // 浏览器侧 window.open
      return;
    }
    if (b.conn_type === "app") {
      if (!target) {
        sendJSON(res, 400, { error: "该品牌未配置应用路径" });
        return;
      }
      const r = launchEndpoint(target);
      sendJSON(res, r.launched ? 200 : 500, r.launched ? { kind: "app", launched: true } : { error: "启动失败: " + r.error });
      return;
    }
    sendJSON(res, 400, { error: "该品牌是托管型（" + b.conn_type + "），在 Agent 列表里管理" });
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
  setProductsRoot(cfg.productsDir);

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
  try {
    switch (cfg.adapterType) {
      case "stdio":
        adapter = await StdioAdapter.create(cfg.localURL, [], cfg.agentID);
        break;
      case "http":
        adapter = await HTTPAdapter.create(cfg.localURL);
        break;
      default:
        logger.error("unknown adapter", { adapter: cfg.adapterType });
        process.exit(1);
    }
  } catch (e) {
    // local 单 Agent 模式没有宿主重试机制：给出可读原因后干净退出，不裸抛堆栈
    logger.error("local agent 启动失败", { adapter: cfg.adapterType, target: cfg.localURL, error: String(e) });
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
