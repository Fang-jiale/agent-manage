import * as proto from "../protocol.ts";
import { AsyncQueue, type LocalAgentAdapter, type LocalAgentEvent } from "./types.ts";

// HTTPAdapter talks to a local agent over HTTP + SSE.
export class HTTPAdapter implements LocalAgentAdapter {
  private baseURL: string;
  private capabilities: proto.Capability[];

  private constructor(baseURL: string, capabilities: proto.Capability[]) {
    this.baseURL = baseURL;
    this.capabilities = capabilities;
  }

  static async create(baseURL: string): Promise<HTTPAdapter> {
    if (!baseURL.startsWith("http://") && !baseURL.startsWith("https://")) {
      baseURL = "http://" + baseURL;
    }
    baseURL = baseURL.replace(/\/+$/, "");

    const resp = await fetch(`${baseURL}/capabilities`);
    const payload = (await resp.json()) as { capabilities?: proto.Capability[] };
    return new HTTPAdapter(baseURL, payload.capabilities ?? []);
  }

  // POSTs the request to /tasks and streams JSON-RPC stream.chunk SSE chunks back.
  // 取消语义：AbortSignal abort 时显式 POST /tasks/:id/cancel，让 local-agent 早停
  // 并兜底发 done:true + reason；不把 signal 直接传给 fetch——否则 abort 一来 SSE 流
  // 立刻断，local-agent 终态 chunk 反而读不到（与 stdio 行为对齐：等 done 才关 queue）。
  // 安全网：cancel POST 后 5s 仍无 done（local-agent 旧版/挂死）才硬 abort 流。
  async send(req: proto.LocalAgentRequest, signal: AbortSignal): Promise<AsyncQueue<proto.LocalAgentChunk>> {
    const hardCtl = new AbortController();
    const taskID = req.task_id;
    const cancelled = { fired: false };
    const onAbort = (): void => {
      if (cancelled.fired || !taskID) return;
      cancelled.fired = true;
      fetch(`${this.baseURL}/tasks/${encodeURIComponent(taskID)}/cancel`, { method: "POST" })
        .catch(() => { /* local-agent 不支持 cancel 端点（旧版），忽略 */ });
      // 5s 安全网：cancel 已发，给 local-agent 时间发完终态 chunk；超时则硬断
      const t = setTimeout(() => hardCtl.abort(), 5000);
      t.unref();
    };
    if (signal.aborted) {
      onAbort();
    } else {
      signal.addEventListener("abort", onAbort, { once: true });
    }

    const resp = await fetch(`${this.baseURL}/tasks`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
      signal: hardCtl.signal,
    });
    if (resp.status !== 200 || !resp.body) {
      throw new Error(`local agent returned HTTP ${resp.status}`);
    }

    const queue = new AsyncQueue<proto.LocalAgentChunk>();
    void (async () => {
      try {
        const decoder = new TextDecoder();
        let buffer = "";
        for await (const part of resp.body!) {
          buffer += decoder.decode(part, { stream: true });
          let idx: number;
          while ((idx = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, idx).trim();
            buffer = buffer.slice(idx + 1);
            this.handleLine(line, req, queue);
          }
        }
        this.handleLine(buffer.trim(), req, queue);
      } catch {
        // Aborted or connection error: end the stream.
      } finally {
        queue.close();
      }
    })();

    return queue;
  }

  private handleLine(line: string, req: proto.LocalAgentRequest, queue: AsyncQueue<proto.LocalAgentChunk>): void {
    if (line === "") return;
    if (line.startsWith("data: ")) line = line.slice(6);

    let msg: proto.Message;
    try {
      msg = JSON.parse(line) as proto.Message;
    } catch {
      return;
    }
    if (msg.method !== "stream.chunk") return;

    const chunk = proto.decodeParams<proto.LocalAgentChunk>(msg);
    if (!chunk.task_id) chunk.task_id = req.task_id;
    if (!chunk.session_id) chunk.session_id = req.session_id;
    if (!chunk.context_id) chunk.context_id = req.context_id;
    queue.push(chunk);
  }

  getCapabilities(): proto.Capability[] {
    return this.capabilities;
  }

  // The HTTP adapter does not support proactive lifecycle events.
  events(): AsyncIterable<LocalAgentEvent> | null {
    return null;
  }

  close(): void {}
}
