import { WebSocket } from "ws";
import * as proto from "../protocol.ts";
import { AsyncQueue, type LocalAgentAdapter, type LocalAgentEvent } from "./types.ts";

// WSAdapter 与本地 Agent 通过 WebSocket 通信（本地 Agent 接口标准 §2.3）：
// AgentClient 作为 WS 客户端连接，消息为单条 JSON（非 JSONL），其余语义与
// StdioAdapter 一致——lifecycle.initialize 协商、stream.chunk 按 task_id 路由、
// abort 时显式补发 task.cancel。

// ack 等待时限，默认 30s（思考型 agent 首包可能较慢）；测试用 YWM_ACK_WAIT_MS 缩短
function ackWaitMs(): number {
  const v = Number(process.env.YWM_ACK_WAIT_MS);
  return Number.isFinite(v) && v > 0 ? v : 30_000;
}

export class WSAdapter implements LocalAgentAdapter {
  private ws: WebSocket;
  private capabilities: proto.Capability[] = [];
  private eventsQueue = new AsyncQueue<LocalAgentEvent>();
  private closed = false;
  // 每个 task_id 独立 queue，语义同 StdioAdapter（create/respond 共用，done 才关）
  private queues = new Map<string, AsyncQueue<proto.LocalAgentChunk>>();
  private cancelledTasks = new Set<string>();
  // task.create / task.respond 的应答等待（ack 透传），语义同 StdioAdapter
  private ackWaiters = new Map<string, {
    resolve: () => void;
    reject: (err: Error & { code?: number }) => void;
    timer: NodeJS.Timeout;
  }>();

  private constructor(ws: WebSocket) {
    this.ws = ws;
  }

  static async create(url: string, agentID?: string): Promise<WSAdapter> {
    const ws = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", (e) => reject(new Error(`ws connect failed: ${e.message}`)));
    });
    const adapter = new WSAdapter(ws);
    ws.on("message", (data) => adapter.handleAgentMessage(data.toString()));
    ws.on("close", () => adapter.onClose());
    ws.on("error", () => adapter.onClose());

    await adapter.handshake(agentID);
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
    // 等待中的 ack 一并判失败（连接断了）
    for (const w of [...this.ackWaiters.values()]) {
      w.reject(Object.assign(new Error("本地 Agent 连接中断"), { code: -32003 }));
    }
  }

  private writeMessage(msg: proto.Message): void {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) throw new Error("adapter is closed");
    this.ws.send(JSON.stringify(msg));
  }

  private handshake(agentID?: string): Promise<void> {
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
        // C3：网关认可的本实例 ID（语义同 StdioAdapter.handshake）
        ...(agentID ? { agentInfo: { agent_id: agentID } } : {}),
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
      case "task.completed": {
        // 兜底：agent 终结任务时只发 event.error / task.completed、不发
        // done:true chunk 的实现，队列会挂到超时——这里合成终止 chunk
        if (msg.method !== "event.notification") {
          const p = (msg.params ?? {}) as { task_id?: string; message?: string; summary?: string };
          const q = p.task_id ? this.queues.get(p.task_id) : undefined;
          if (q) {
            this.queues.delete(p.task_id!);
            // 终结消息同样视为隐式 ack
            this.ackWaiters.get(p.task_id!)?.resolve();
            const failed = msg.method === "event.error";
            q.push({
              task_id: p.task_id,
              type: "text",
              content: [{ type: "text", text: failed ? `任务失败：${p.message ?? "unknown error"}` : (p.summary ?? "") }],
              error: failed ? (p.message ?? "unknown error") : undefined,
              reason: failed ? "error" : undefined,
              done: true,
            });
            q.close();
          }
        }
        this.eventsQueue.push({ method: msg.method, params: msg.params });
        break;
      }
      case proto.METHOD_TASK_INVOKE:
        // 管理者编排请求：带 id 上抛，client 桥接到网关后须回响应
        this.eventsQueue.push({
          method: msg.method,
          params: msg.params,
          id: msg.id !== undefined && msg.id !== null ? String(msg.id) : undefined,
        });
        break;
      default: {
        // JSON-RPC 响应：task.create / task.respond 的 ack 或错误，路由给等待者
        const waiter = msg.id !== undefined && msg.id !== null ? this.ackWaiters.get(String(msg.id)) : undefined;
        if (waiter) {
          if (msg.error) {
            waiter.reject(Object.assign(new Error(msg.error.message || "agent 拒绝了请求"), { code: msg.error.code }));
          } else {
            waiter.resolve();
          }
        }
      }
    }
  }

  private routeChunk(chunk: proto.LocalAgentChunk): void {
    const taskID = chunk.task_id;
    if (!taskID) return;
    // 隐式 ack：不回 task.create 应答的存量 agent，首个 chunk 即接受证据
    this.ackWaiters.get(taskID)?.resolve();
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

    // 等 agent 应答（显式 ack / 错误响应 / 隐式首 chunk），语义同 StdioAdapter
    await this.waitAck(req.task_id, signal);
    return q;
  }

  private waitAck(taskID: string, signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const entry: {
        resolve: () => void;
        reject: (err: Error & { code?: number }) => void;
        timer: NodeJS.Timeout;
      } = {
        resolve: () => {
          clearTimeout(entry.timer);
          if (this.ackWaiters.get(taskID) === entry) this.ackWaiters.delete(taskID);
          resolve();
        },
        reject: (err) => {
          clearTimeout(entry.timer);
          if (this.ackWaiters.get(taskID) === entry) this.ackWaiters.delete(taskID);
          reject(err);
        },
        timer: undefined as unknown as NodeJS.Timeout,
      };
      entry.timer = setTimeout(() => {
        entry.reject(Object.assign(
          new Error(`本地 Agent 无应答（${Math.round(ackWaitMs() / 1000)}s 内无 ack 且无输出）`),
          { code: -32001 },
        ));
      }, ackWaitMs());
      entry.timer.unref();
      this.ackWaiters.set(taskID, entry);
      const onAbort = (): void => {
        if (this.ackWaiters.get(taskID) === entry) entry.resolve();
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
    });
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
