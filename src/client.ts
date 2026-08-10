import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocket } from "ws";
import * as proto from "./protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, setLogLevel, logger } from "./util.ts";
import { HTTPAdapter } from "./adapters/http.ts";
import { StdioAdapter } from "./adapters/stdio.ts";
import type { LocalAgentAdapter, LocalAgentEvent } from "./adapters/types.ts";

interface ClientConfig {
  gateway: string;
  agentID: string;
  localURL: string;
  adapterType: string;
  token: string;
  deviceKey: string;
  logLevel: string;
  taskTimeoutMs: number;
}

function loadClientConfig(): ClientConfig {
  const specs = [
    { name: "gateway", type: "string" as const, default: envString("AGENT_MANAGE_GATEWAY", "ws://localhost:8080/ws/agent") },
    { name: "agent-id", type: "string" as const, default: envString("AGENT_MANAGE_AGENT_ID", "") },
    { name: "local-agent", type: "string" as const, default: envString("AGENT_MANAGE_LOCAL_AGENT", "http://localhost:9001") },
    { name: "adapter", type: "string" as const, default: envString("AGENT_MANAGE_ADAPTER", "http") },
    { name: "token", type: "string" as const, default: envString("AGENT_MANAGE_TOKEN", "") },
    { name: "key", type: "string" as const, default: envString("AGENT_MANAGE_DEVICE_KEY", "") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 300_000)) },
  ];
  const values = parseFlags(specs);
  // 二选一：长期运行的 agent 推荐设备密钥（-key），JWT 会过期
  if (values["token"] === "" && values["key"] === "") {
    console.error("缺少认证：-key <设备密钥>（管理后台创建，推荐）或 -token <用户 JWT>（node src/login.ts 获取）");
    process.exit(1);
  }
  if (values["token"] !== "" && values["key"] !== "") {
    console.error("-token 与 -key 只能二选一");
    process.exit(1);
  }
  let taskTimeoutMs = Number(values["task-timeout"]);
  if (Number.isNaN(taskTimeoutMs)) {
    taskTimeoutMs = parseDurationMs(values["task-timeout"]) ?? 300_000;
  }
  return {
    gateway: values["gateway"],
    agentID: values["agent-id"],
    localURL: values["local-agent"],
    adapterType: values["adapter"],
    token: values["token"],
    deviceKey: values["key"],
    logLevel: values["log-level"],
    taskTimeoutMs,
  };
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
    sendStatus(ws, agentID, proto.AGENT_STATUS_ONLINE);
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

  const controller = tasks.add(params.task_id);
  try {
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

    let chunks: AsyncIterable<proto.LocalAgentChunk>;
    try {
      chunks = await adapter.send(req, controller.signal);
    } catch (err) {
      const errMsg = controller.signal.aborted && controller.signal.reason === "timeout"
        ? "timeout"
        : err instanceof Error ? err.message : String(err);
      sendProgress(ws, agentID, params.task_id, params.session_id, undefined,
        proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: errMsg, done: true });
      return;
    }

    for await (const chunk of chunks) {
      const kind = chunk.done ? proto.PROGRESS_KIND_END : proto.PROGRESS_KIND_REPORT;
      sendProgress(ws, agentID, chunk.task_id ?? params.task_id, chunk.session_id ?? params.session_id,
        chunk.context_id, kind, chunk);
    }
    if (controller.signal.aborted && controller.signal.reason === "timeout") {
      sendProgress(ws, agentID, params.task_id, params.session_id, undefined,
        proto.PROGRESS_KIND_END, { type: proto.CHUNK_TYPE_TEXT, error: "timeout", done: true });
    }
  } finally {
    tasks.remove(params.task_id);
  }
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

async function main(): Promise<void> {
  const cfg = loadClientConfig();
  setLogLevel(cfg.logLevel);

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
  process.on("SIGINT", () => {
    interrupted = true;
  });

  // Bridge lifecycle events from the local agent to the current gateway connection.
  let currentWS: WebSocket | null = null;
  const events = adapter.events();
  if (events) {
    void (async () => {
      for await (const ev of events) {
        const msg = translateLifecycleEvent(cfg.agentID, ev);
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
    }, 30_000);
    heartbeat.unref();

    ws.on("message", (data) => {
      let msg: proto.Message;
      try {
        msg = JSON.parse(data.toString()) as proto.Message;
      } catch {
        return;
      }
      switch (msg.method) {
        case proto.METHOD_AGENT_CHAT:
          void handleChat(ws, adapter, cfg.agentID, msg, tasks);
          break;
        case proto.METHOD_AGENT_RESPOND:
          void handleRespond(ws, adapter, cfg.agentID, msg, tasks);
          break;
        case proto.METHOD_AGENT_CANCEL: {
          const params = proto.decodeParams<proto.AgentCancelParams>(msg);
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

    await new Promise<void>((resolve) => {
      ws.once("close", () => resolve());
      const onSigint = (): void => {
        ws.close(1000);
        resolve();
      };
      process.once("SIGINT", onSigint);
    });

    clearInterval(heartbeat);
    currentWS = null;
    if (interrupted) {
      logger.info("interrupt received, exiting...");
      break;
    }
    logger.info("connection lost, reconnecting...");
    tasks.cancelAll();
    await sleep(reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
  }

  adapter.close();
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((err) => {
    logger.error("client failed", { error: err instanceof Error ? err.message : String(err) });
    process.exit(1);
  });
}
