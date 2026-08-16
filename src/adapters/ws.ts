import { WebSocket } from "ws";
import * as proto from "../protocol.ts";
import { AsyncQueue, type LocalAgentAdapter, type LocalAgentEvent } from "./types.ts";

// WSAdapter 与本地 Agent 通过 WebSocket 通信（本地 Agent 接口标准 §2.3）：
// AgentClient 作为 WS 客户端连接，消息为单条 JSON（非 JSONL），其余语义与
// StdioAdapter 一致——lifecycle.initialize 协商、stream.chunk 按 task_id 路由、
// abort 时显式补发 task.cancel。
export class WSAdapter implements LocalAgentAdapter {
  private ws: WebSocket;
  private capabilities: proto.Capability[] = [];
  private eventsQueue = new AsyncQueue<LocalAgentEvent>();
  private closed = false;
  // 每个 task_id 独立 queue，语义同 StdioAdapter（create/respond 共用，done 才关）
  private queues = new Map<string, AsyncQueue<proto.LocalAgentChunk>>();
  private cancelledTasks = new Set<string>();

  private constructor(ws: WebSocket) {
    this.ws = ws;
  }

  static async create(url: string): Promise<WSAdapter> {
    const ws = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", (e) => reject(new Error(`ws connect failed: ${e.message}`)));
    });
    const adapter = new WSAdapter(ws);
    ws.on("message", (data) => adapter.handleAgentMessage(data.toString()));
    ws.on("close", () => adapter.onClose());
    ws.on("error", () => adapter.onClose());

    await adapter.handshake();
    // 等一小段 lifecycle.register（非强制）
    await adapter.waitRegister(5000);
    return adapter;
  }

  private onClose(): void {
    if (this.closed) return;
    this.closed = true;
    this.eventsQueue.close();
    for (const q of this.queues.values()) q.close();
    this.queues.clear();
  }

  private writeMessage(msg: proto.Message): void {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) throw new Error("adapter is closed");
    this.ws.send(JSON.stringify(msg));
  }

  private handshake(): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("no response to lifecycle.initialize")), 10_000);
      const onMessage = (data: WebSocket.RawData): void => {
        let msg: proto.Message;
        try {
          msg = JSON.parse(data.toString()) as proto.Message;
        } catch {
          return;
        }
        if (msg.id === "init-1") {
          clearTimeout(timer);
          this.ws.off("message", onMessage);
          if (msg.error) {
            reject(new Error(`initialize error: json-rpc error ${msg.error.code}: ${msg.error.message}`));
            return;
          }
          if (this.capabilities.length === 0) {
            this.capabilities = [{ type: "chat", name: "general", description: "通用对话能力" }];
          }
          this.writeMessage(proto.newNotification(proto.METHOD_LIFECYCLE_INITIALIZED, {}));
          resolve();
        }
      };
      this.ws.on("message", onMessage);
      this.writeMessage(proto.newRequest("init-1", proto.METHOD_LIFECYCLE_INITIALIZE, {
        protocolVersion: "1.0.0",
        capabilities: { chat: {}, streaming: {}, confirmations: {}, prompts: {} },
        clientInfo: { name: "agent-client", version: "1.0.0" },
      } satisfies proto.InitializeParams));
    });
  }

  // register 可能在 initialize 响应前后到达；给它一个短窗口
  private waitRegister(timeoutMs: number): Promise<void> {
    if (this.gotRegister) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, timeoutMs);
      timer.unref();
      this.registerWaiter = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  private gotRegister = false;
  private registerWaiter?: () => void;

  private handleAgentMessage(raw: string): void {
    let msg: proto.Message;
    try {
      msg = JSON.parse(raw) as proto.Message;
    } catch {
      return;
    }
    switch (msg.method) {
      case "stream.chunk": {
        const chunk = proto.decodeParams<proto.LocalAgentChunk>(msg);
        this.routeChunk(chunk);
        break;
      }
      case proto.METHOD_LIFECYCLE_REGISTER: {
        const params = proto.decodeParams<proto.RegisterParams>(msg);
        this.capabilities = params.capabilities ?? [];
        this.gotRegister = true;
        this.registerWaiter?.();
        this.registerWaiter = undefined;
        this.eventsQueue.push({ method: msg.method, params: msg.params });
        break;
      }
      case proto.METHOD_LIFECYCLE_STATUS:
      case proto.METHOD_LIFECYCLE_CAPABILITIES_UPDATED:
      case "event.notification":
      case "event.error":
      case "task.completed":
        this.eventsQueue.push({ method: msg.method, params: msg.params });
        break;
      case proto.METHOD_TASK_INVOKE:
        // 管理者编排请求：带 id 上抛，client 桥接到网关后须回响应
        this.eventsQueue.push({
          method: msg.method,
          params: msg.params,
          id: msg.id !== undefined && msg.id !== null ? String(msg.id) : undefined,
        });
        break;
      default:
        // 响应（init/task.create 的 ack 等）与未知方法忽略
    }
  }

  private routeChunk(chunk: proto.LocalAgentChunk): void {
    const taskID = chunk.task_id;
    if (!taskID) return;
    const q = this.queues.get(taskID);
    if (!q) return;
    q.push(chunk);
    if (chunk.done) {
      this.queues.delete(taskID);
      this.cancelledTasks.delete(taskID);
      q.close();
    }
  }

  // 语义同 StdioAdapter.send：同一 task 的 create/respond 共用 queue；abort 时
  // 显式补发 task.cancel（不立即关 queue，等 agent 兜底发 confirm_cancelled + done）。
  async send(req: proto.LocalAgentRequest, signal: AbortSignal): Promise<AsyncIterable<proto.LocalAgentChunk>> {
    if (this.closed) throw new Error("adapter is closed");
    if (!req.task_id) throw new Error("send: missing task_id");

    const method = req.type === "respond" ? proto.METHOD_TASK_RESPOND : proto.METHOD_TASK_CREATE;
    this.writeMessage(proto.newRequest(req.task_id, method, req));

    let q = this.queues.get(req.task_id);
    if (!q) {
      q = new AsyncQueue<proto.LocalAgentChunk>();
      this.queues.set(req.task_id, q);
    }

    const taskID = req.task_id;
    const onAbort = (): void => {
      if (this.cancelledTasks.has(taskID)) return;
      this.cancelledTasks.add(taskID);
      if (!this.closed) {
        try {
          this.writeMessage(proto.newNotification(proto.METHOD_TASK_CANCEL, {
            task_id: taskID,
            session_id: req.session_id,
          }));
        } catch { /* 连接已断，忽略 */ }
      }
    };
    if (signal.aborted) {
      onAbort();
    } else {
      signal.addEventListener("abort", onAbort, { once: true });
    }

    return q;
  }

  getCapabilities(): proto.Capability[] {
    return this.capabilities;
  }

  sendToAgent(msg: proto.Message): boolean {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) return false;
    try {
      this.writeMessage(msg);
      return true;
    } catch {
      return false;
    }
  }

  events(): AsyncIterable<LocalAgentEvent> {
    return this.eventsQueue;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      this.ws.close();
    } catch { /* already closed */ }
    for (const q of this.queues.values()) q.close();
    this.queues.clear();
    this.eventsQueue.close();
  }
}
