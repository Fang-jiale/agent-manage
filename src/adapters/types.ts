import type { Capability, LocalAgentChunk, LocalAgentRequest, Message } from "../protocol.ts";

// LocalAgentEvent is a lifecycle or event notification emitted by a local agent.
// id 非空表示这是一条请求（如 task.invoke），AgentClient 需回 JSON-RPC 响应。
export interface LocalAgentEvent {
  method: string;
  params: unknown;
  id?: string;
}

// LocalAgentAdapter abstracts how AgentClient talks to a local agent.
export interface LocalAgentAdapter {
  // send writes a task request (task.create / task.respond) to the local agent
  // and returns the chunk stream for that task.
  //
  // 同一 task_id 的多次 send()（典型：先 task.create 后 task.respond）返回同一
  // 个 chunk 流——下游消费者（client.ts handleChat）跨轮次共享，task.respond 的
  // 后续 chunk 自动归到原消费者。多次 send() 通常仅首次迭代，后续调用方只关心
  // "把请求写出去"。
  //
  // 取消语义：传入的 AbortSignal abort 时，adapter 必须向 local agent 补发
  // task.cancel（stdio 子进程必须收到否则是假停止）；chunk 流不立即关闭，等 agent
  // 兜底发完 confirm_cancelled + done:true 再自然结束。
  send(req: LocalAgentRequest, signal: AbortSignal): Promise<AsyncIterable<LocalAgentChunk>>;

  // Capabilities returns the capabilities advertised by the local agent.
  getCapabilities(): Capability[];

  // Events returns a stream of lifecycle / event notifications from the local
  // agent, or null when the adapter does not support proactive events (HTTP).
  events(): AsyncIterable<LocalAgentEvent> | null;

  // sendToAgent 向本地 Agent 写一条下行消息（task.subtask_result 通知、
  // task.invoke 的响应）。仅 stdio/ws 适配器实现；HTTP 无下行通道，返回
  // false/未实现——编排能力只在支持主动下行的适配器上可用。
  sendToAgent?(msg: Message): boolean;

  // Close releases any resources held by the adapter.
  close(): void | Promise<void>;
}

// AsyncQueue is a push-based async iterable used to deliver stream chunks and
// events to consumers.
export class AsyncQueue<T> implements AsyncIterable<T> {
  private items: T[] = [];
  private waiters: ((result: IteratorResult<T>) => void)[] = [];
  private closed = false;

  push(item: T): void {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value: item, done: false });
    } else {
      this.items.push(item);
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined as T, done: true });
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        const item = this.items.shift();
        if (item !== undefined) return Promise.resolve({ value: item, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as T, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}
