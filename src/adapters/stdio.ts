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
// shell 风格分词：支持单/双引号包裹含空格的路径；中文输入法易把引号打成
// 弯引号（‘’“”），先归一化成 ASCII 引号再解析，否则整段参数被字面带进引号。
function tokenizeCommand(cmd: string): string[] {
  const s = cmd.replace(/[‘’]/g, "'").replace(/[“”]/g, '"');
  const parts: string[] = [];
  let cur = "";
  let quote: string | undefined;
  let has = false;
  for (const ch of s) {
    if (quote) {
      if (ch === quote) quote = undefined;
      else cur += ch;
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      has = true;
      continue;
    }
    if (/\s/.test(ch)) {
      if (cur !== "" || has) {
        parts.push(cur);
        cur = "";
        has = false;
      }
      continue;
    }
    cur += ch;
  }
  if (cur !== "" || has) parts.push(cur);
  return parts;
}

// Windows 上 .cmd/.bat（含依赖 PATHEXT 解析的 npm/npx）自 CVE-2024-27980 修复后
// 不允许被无 shell 的 spawn 直接执行，报 EINVAL——提示用户换 cmd /c 或 bash 包装
function spawnErrorHint(e: unknown): string {
  const msg = e instanceof Error ? e.message : String(e);
  if (process.platform === "win32" && (e as NodeJS.ErrnoException)?.code === "EINVAL") {
    return `${msg}（Windows 不能直接执行 .cmd/.bat，请改用 "cmd /c <命令>" 包装，或用 git bash / node 直接启动）`;
  }
  return msg;
}

// ack 等待时限，默认 30s（思考型 agent 首包可能较慢）；测试用 YWM_ACK_WAIT_MS 缩短
function ackWaitMs(): number {
  const v = Number(process.env.YWM_ACK_WAIT_MS);
  return Number.isFinite(v) && v > 0 ? v : 30_000;
}

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
  // task.create / task.respond 的应答等待（ack 透传）：显式 JSON-RPC 响应或
  // 首个 stream.chunk（隐式接受）在时限内到达即放行；错误响应携带 code 抛出。
  // 测试可用 YWM_ACK_WAIT_MS 缩短时限。
  private ackWaiters = new Map<string, {
    resolve: () => void;
    reject: (err: Error & { code?: number }) => void;
    timer: NodeJS.Timeout;
  }>();

  private constructor(proc: ChildProcess) {
    this.proc = proc;
  }

  static async create(command: string, args: string[] = [], agentID?: string): Promise<StdioAdapter> {
    const parts = tokenizeCommand(command);
    if (parts.length > 0) {
      command = parts[0];
      args = [...parts.slice(1), ...args];
    }

    let proc: ChildProcess;
    try {
      proc = spawn(command, args, { stdio: ["pipe", "pipe", "inherit"] });
    } catch (e) {
      throw new Error(`spawn "${command}" failed: ${spawnErrorHint(e)}`);
    }
    const adapter = new StdioAdapter(proc);

    const rl = readline.createInterface({ input: proc.stdout! });
    rl.on("line", (line) => adapter.dispatchLine(line));
    proc.on("exit", () => adapter.onExit());
    proc.on("error", () => adapter.onExit());
    // 子进程秒退后再写 stdin 会异步抛 EPIPE/ERR_STREAM_DESTROYED；
    // 不挂 error 监听会变成 uncaught exception，直接炸掉整个 client 进程
    proc.stdin?.on("error", () => { /* 关闭由 onExit 兜底 */ });
    proc.stdout?.on("error", () => { /* 同上 */ });

    await adapter.handshake(agentID);

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
    // 等待中的 ack 一并判失败（agent 进程没了）
    for (const w of [...this.ackWaiters.values()]) {
      w.reject(Object.assign(new Error("本地 Agent 连接中断"), { code: -32003 }));
    }
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

  private async handshake(agentID?: string): Promise<void> {
    this.writeMessage(proto.newRequest("init-1", proto.METHOD_LIFECYCLE_INITIALIZE, {
      protocolVersion: "1.0.0",
      capabilities: {
        chat: {},
        streaming: {},
        confirmations: {},
        prompts: {},
      },
      clientInfo: { name: "agent-client", version: "1.0.0" },
      // C3：网关认可的本实例 ID，先于任何任务下发；shim 做受信判断以此为准
      ...(agentID ? { agentInfo: { agent_id: agentID } } : {}),
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
      case "event.notification": {
        this.eventsQueue.push({ method: msg.method, params: msg.params });
        break;
      }
      case "event.error":
      case "task.completed": {
        // 兜底：agent 终结任务时只发 event.error / task.completed、不发
        // done:true chunk 的实现，队列会挂到超时——这里合成终止 chunk。
        // 注意 lifecycle.status 不在此列：shim 会在任务开始时发 status:busy
        // （带 task_id），若也合成 done 会在任务起步瞬间就把它终结掉。
        {
          const p = (msg.params ?? {}) as { task_id?: string; message?: string; summary?: string };
          const q = p.task_id ? this.queues.get(p.task_id) : undefined;
          if (q) {
            this.queues.delete(p.task_id!);
            // 终结消息同样视为隐式 ack（不回 task.create 应答的存量 agent）
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
        // JSON-RPC 响应：task.create / task.respond 的 ack 或错误，路由给等待者；
        // 无人等待则忽略（生命周期早期或未知来源的消息）
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
    if (!taskID) return; // shim 必须在 chunk 里带 task_id，否则无法路由
    // 隐式 ack：不回 task.create 应答的存量 agent，首个 chunk 即接受证据
    this.ackWaiters.get(taskID)?.resolve();
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

    // 等 agent 应答（显式 ack / 错误响应 / 隐式首 chunk）；拒绝与超时在此抛出，
    // client 得以在任务创建前把错误透传回网关，而不是无声悬挂
    await this.waitAck(req.task_id, signal);
    return q;
  }

  // 登记一个 task 的 ack 等待。resolve/reject 自清理；abort 时静默放行
  // （后续由 task.cancel 兜底流程收尾）。
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
    if (this.closed) return false;
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
      this.proc.stdin!.end();
    } catch { /* already closed */ }
    this.proc.kill();
  }
}
