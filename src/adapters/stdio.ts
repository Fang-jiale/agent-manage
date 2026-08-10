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
  private currentQueue: AsyncQueue<proto.LocalAgentChunk> | null = null;

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
    const q = this.currentQueue;
    this.currentQueue = null;
    q?.close();
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
    const q = this.currentQueue;
    if (!q) {
      // send() 已返回（典型：confirm_required 关 queue 后用户点停止，
      // shim 兜底发 confirm_cancelled）。这类"事后"chunk 必须走 events 流
      // 转发到 gateway，否则前端永远收不到撤销通知。
      if (chunk.type === proto.CHUNK_TYPE_CONFIRM_CANCELLED) {
        this.eventsQueue.push({ method: "stream.chunk", params: chunk });
      }
      return;
    }
    q.push(chunk);
    if (
      chunk.done ||
      chunk.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED ||
      chunk.type === proto.CHUNK_TYPE_PROMPT_REQUIRED ||
      chunk.type === proto.CHUNK_TYPE_BLOCK_REQUIRED
    ) {
      this.currentQueue = null;
      q.close();
    }
  }

  // Send writes a JSON-RPC task request to the agent's stdin and reads chunks
  // from stdout. Stdio agents are assumed to handle one request at a time.
  async send(req: proto.LocalAgentRequest, signal: AbortSignal): Promise<AsyncIterable<proto.LocalAgentChunk>> {
    if (this.currentQueue) throw new Error("another request is in progress");
    if (this.closed) throw new Error("adapter is closed");

    const method = req.type === "respond" ? proto.METHOD_TASK_RESPOND : proto.METHOD_TASK_CREATE;
    this.writeMessage(proto.newRequest(req.task_id ?? "", method, req));

    const queue = new AsyncQueue<proto.LocalAgentChunk>();
    this.currentQueue = queue;

    const onAbort = (): void => {
      // 关键：必须显式通知子进程取消，否则 shim 和被控进程继续跑、烧 token。
      // 仅中断本地 AbortController 是假停止。HTTP 适配器不通过此路径。
      // 注意：confirm_required 已经把 currentQueue 设为 null，所以这里要无条件转发
      // task.cancel——只要还知道 task_id，shim 就该收到。
      if (this.currentQueue === queue) {
        this.currentQueue = null;
        queue.close();
      }
      if (!this.closed && req.task_id) {
        try {
          this.writeMessage(proto.newNotification(proto.METHOD_TASK_CANCEL, {
            task_id: req.task_id,
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

    return queue;
  }

  getCapabilities(): proto.Capability[] {
    return this.capabilities;
  }

  cancelTask(taskID: string, sessionID?: string): void {
    if (this.closed) return;
    try {
      this.writeMessage(proto.newNotification(proto.METHOD_TASK_CANCEL, {
        task_id: taskID,
        session_id: sessionID,
      }));
    } catch {
      // stdin 已关闭等场景，忽略
    }
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
