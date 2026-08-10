import { spawn, type ChildProcess } from "node:child_process";
import readline from "node:readline";
import * as proto from "../protocol.ts";
import { AsyncQueue, type LocalAgentAdapter, type LocalAgentEvent } from "./types.ts";

interface PendingLine {
  resolve: (line: string | undefined) => void;
  timer?: NodeJS.Timeout;
}

// StdioAdapter talks to a local agent by spawning it as a subprocess and
// communicating over stdin/stdout using JSON-RPC over JSON lines.
export class StdioAdapter implements LocalAgentAdapter {
  private proc: ChildProcess;
  private capabilities: proto.Capability[] = [];
  private eventsQueue = new AsyncQueue<LocalAgentEvent>();
  private lines: string[] = [];
  private lineWaiters: PendingLine[] = [];
  private closed = false;
  // 每个 task_id 独立 queue。chunk 流与 send() 解耦：
  // - 同一 task 的 task.create / task.respond 共用同一 queue
  // - 多个 task 可并发（stdio 子进程若支持）
  // - queue 只在 done:true 或 abort+cancel 后关闭，confirm_required 不再提前关
  private queues = new Map<string, AsyncQueue<proto.LocalAgentChunk>>();
  // 已转发过 task.cancel 的 task_id 集合。多次 send() 复用同一 controller.signal，
  // abort 回调可能注册多份；这里去重避免给 shim 重复发取消通知。
  private cancelledTasks = new Set<string>();

  private constructor(proc: ChildProcess) {
    this.proc = proc;
  }

  static async create(command: string, args: string[] = []): Promise<StdioAdapter> {
    const parts = command.split(/\s+/).filter((p) => p.length > 0);
    if (parts.length > 0) {
      command = parts[0];
      args = [...parts.slice(1), ...args];
    }

    const proc = spawn(command, args, { stdio: ["pipe", "pipe", "inherit"] });
    const adapter = new StdioAdapter(proc);

    const rl = readline.createInterface({ input: proc.stdout! });
    rl.on("line", (line) => adapter.dispatchLine(line));
    proc.on("exit", () => adapter.onExit());
    proc.on("error", () => adapter.onExit());

    await adapter.handshake();

    // Try to read the lifecycle.register notification that the agent should
    // send shortly after initialization. Non-fatal if it doesn't arrive.
    await adapter.readInitialRegister(5000);

    // Start the main event loop.
    void adapter.run();

    return adapter;
  }

  private dispatchLine(line: string): void {
    const waiter = this.lineWaiters.shift();
    if (waiter) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(line);
    } else {
      this.lines.push(line);
    }
  }

  private onExit(): void {
    this.closed = true;
    for (const waiter of this.lineWaiters.splice(0)) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(undefined);
    }
    this.eventsQueue.close();
    // 关掉所有 in-flight queue——消费者会收到 iterator end
    for (const q of this.queues.values()) q.close();
    this.queues.clear();
  }

  private readLine(timeoutMs: number): Promise<string | undefined> {
    const line = this.lines.shift();
    if (line !== undefined) return Promise.resolve(line);
    if (this.closed) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      const pending: PendingLine = { resolve };
      if (timeoutMs > 0) {
        pending.timer = setTimeout(() => {
          const idx = this.lineWaiters.indexOf(pending);
          if (idx !== -1) this.lineWaiters.splice(idx, 1);
          resolve(undefined);
        }, timeoutMs);
        pending.timer.unref();
      }
      this.lineWaiters.push(pending);
    });
  }

  private writeMessage(msg: proto.Message): void {
    if (this.closed) throw new Error("adapter is closed");
    this.proc.stdin!.write(JSON.stringify(msg) + "\n");
  }

  private async handshake(): Promise<void> {
    this.writeMessage(proto.newRequest("init-1", proto.METHOD_LIFECYCLE_INITIALIZE, {
      protocolVersion: "1.0.0",
      capabilities: {
        chat: {},
        streaming: {},
        confirmations: {},
        prompts: {},
      },
      clientInfo: { name: "agent-client", version: "1.0.0" },
    } satisfies proto.InitializeParams));

    for (;;) {
      const line = await this.readLine(10_000);
      if (line === undefined) throw new Error("no response to lifecycle.initialize");
      const trimmed = line.trim();
      if (trimmed === "") continue;
      let msg: proto.Message;
      try {
        msg = JSON.parse(trimmed) as proto.Message;
      } catch {
        continue;
      }
      if (msg.id === "init-1" && msg.result !== undefined) {
        if (this.capabilities.length === 0) {
          this.capabilities = [{ type: "chat", name: "general", description: "通用对话能力" }];
        }
        break;
      }
      if (msg.error) {
        throw new Error(`initialize error: json-rpc error ${msg.error.code}: ${msg.error.message}`);
      }
    }

    this.writeMessage(proto.newNotification(proto.METHOD_LIFECYCLE_INITIALIZED, {}));
  }

  private async readInitialRegister(timeoutMs: number): Promise<void> {
    const line = await this.readLine(timeoutMs);
    if (line === undefined) return;
    this.handleAgentMessage(line.trim());
  }

  private async run(): Promise<void> {
    for (;;) {
      const line = await this.readLine(0);
      if (line === undefined) return;
      this.handleAgentMessage(line.trim());
    }
  }

  private handleAgentMessage(line: string): void {
    if (line === "") return;
    let msg: proto.Message;
    try {
      msg = JSON.parse(line) as proto.Message;
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
      default:
        // Ignore unrecognized messages.
    }
  }

  private routeChunk(chunk: proto.LocalAgentChunk): void {
    const taskID = chunk.task_id;
    if (!taskID) return; // shim 必须在 chunk 里带 task_id，否则无法路由
    const q = this.queues.get(taskID);
    if (!q) return; // 未知 task（已结束 / 漏发 create）——丢弃
    q.push(chunk);
    if (chunk.done) {
      // 仅 done:true 关 queue；confirm_required / prompt_required / block_required
      // 不再提前关——同一 task 后续 task.respond 的 chunk 流回同一 queue。
      this.queues.delete(taskID);
      this.cancelledTasks.delete(taskID);
      q.close();
    }
  }

  // send 写一条 task.create / task.respond 请求到 shim stdin，返回该 task 的
  // chunk 流。多次 send() 复用同一 task_id 时返回同一 queue（仅首个调用方应
  // 迭代；后续调用方仅做"写请求"动作，迭代由首个调用方继续）。
  //
  // 何时关闭 queue：见 routeChunk（done:true）与 onAbort（abort 不立即关，
  // 等 shim 兜底发完 confirm_cancelled + done）。
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
      // 关键：必须显式通知子进程取消，否则 shim 和被控进程继续跑、烧 token。
      // 仅中断本地 AbortController 是假停止。HTTP 适配器不通过此路径。
      // 这里不主动关 queue——shim 收到 task.cancel 后会兜底发 confirm_cancelled
      // + done:true，让前端有机会收到撤销通知；done 一到 queue 自然关闭。
      if (this.cancelledTasks.has(taskID)) return;
      this.cancelledTasks.add(taskID);
      if (!this.closed) {
        try {
          this.writeMessage(proto.newNotification(proto.METHOD_TASK_CANCEL, {
            task_id: taskID,
            session_id: req.session_id,
          }));
        } catch {
          // stdin 已关闭等场景，忽略
        }
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

  events(): AsyncIterable<LocalAgentEvent> {
    return this.eventsQueue;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      this.proc.stdin!.end();
    } catch { /* already closed */ }
    this.proc.kill();
  }
}
