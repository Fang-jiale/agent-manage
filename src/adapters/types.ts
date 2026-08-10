import type { Capability, LocalAgentChunk, LocalAgentRequest } from "../protocol.ts";

// LocalAgentEvent is a lifecycle or event notification emitted by a local agent.
export interface LocalAgentEvent {
  method: string;
  params: unknown;
}

// LocalAgentAdapter abstracts how AgentClient talks to a local agent.
export interface LocalAgentAdapter {
  // Send forwards a user message or response to the local agent and returns a
  // stream of chunks.
  send(req: LocalAgentRequest, signal: AbortSignal): Promise<AsyncIterable<LocalAgentChunk>>;

  // Capabilities returns the capabilities advertised by the local agent.
  getCapabilities(): Capability[];

  // Events returns a stream of lifecycle / event notifications from the local
  // agent, or null when the adapter does not support proactive events (HTTP).
  events(): AsyncIterable<LocalAgentEvent> | null;

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
