import { test } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { WebSocketServer } from "ws";
import { WSAdapter } from "../src/adapters/ws.ts";
import * as proto from "../src/protocol.ts";
import { setLogLevel } from "../src/util.ts";

setLogLevel("error");

// 最小 WS 本地 Agent：应答 initialize、发 register、task.create 回两个 chunk 后 done
async function startFakeAgent(): Promise<{ url: string; close: () => void }> {
  const wss = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  await new Promise<void>((resolve) => wss.on("listening", resolve));
  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      const msg = JSON.parse(data.toString()) as proto.Message;
      if (msg.method === proto.METHOD_LIFECYCLE_INITIALIZE) {
        ws.send(JSON.stringify({
          jsonrpc: "2.0", id: msg.id,
          result: { protocolVersion: "1.0.0", capabilities: { chat: {} }, serverInfo: { name: "fake", version: "0" } },
        }));
        ws.send(JSON.stringify({
          jsonrpc: "2.0", method: proto.METHOD_LIFECYCLE_REGISTER,
          params: { agent_id: "fake", capabilities: [{ type: "chat", name: "fake-cap", description: "测试能力" }] },
        }));
        return;
      }
      if (msg.method === proto.METHOD_TASK_CREATE) {
        const params = msg.params as { task_id: string; content?: string };
        ws.send(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: { task_id: params.task_id, status: "accepted" } }));
        ws.send(JSON.stringify({
          jsonrpc: "2.0", method: "stream.chunk",
          params: { task_id: params.task_id, type: "text", content: [{ type: "text", text: `echo:${params.content}` }] },
        }));
        ws.send(JSON.stringify({
          jsonrpc: "2.0", method: "stream.chunk",
          params: { task_id: params.task_id, type: "text", done: true },
        }));
      }
    });
  });
  const port = (wss.address() as AddressInfo).port;
  return { url: `ws://127.0.0.1:${port}`, close: () => wss.close() };
}

test("WSAdapter：initialize 握手 + register 能力 + task chunk 流", async () => {
  const fake = await startFakeAgent();
  const adapter = await WSAdapter.create(fake.url);
  try {
    // register 上报的能力
    assert.deepEqual(adapter.getCapabilities().map((c) => c.name), ["fake-cap"]);

    const controller = new AbortController();
    const chunks = await adapter.send(
      { task_id: "t1", type: "chat", content: "hello" } as proto.LocalAgentRequest,
      controller.signal,
    );
    const texts: string[] = [];
    let sawDone = false;
    for await (const chunk of chunks) {
      if (chunk.done) sawDone = true;
      const items = chunk.content as { text?: string }[] | undefined;
      if (items?.[0]?.text) texts.push(items[0].text);
    }
    assert.deepEqual(texts, ["echo:hello"]);
    assert.ok(sawDone);
  } finally {
    adapter.close();
    fake.close();
  }
});

test("WSAdapter：abort 时向 agent 补发 task.cancel", async () => {
  const wss = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  await new Promise<void>((resolve) => wss.on("listening", resolve));
  let cancelled = "";
  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      const msg = JSON.parse(data.toString()) as proto.Message;
      if (msg.method === proto.METHOD_LIFECYCLE_INITIALIZE) {
        ws.send(JSON.stringify({ jsonrpc: "2.0", id: msg.id, result: { protocolVersion: "1.0.0", capabilities: {}, serverInfo: { name: "fake", version: "0" } } }));
        return;
      }
      if (msg.method === proto.METHOD_TASK_CANCEL) {
        cancelled = (msg.params as { task_id: string }).task_id;
      }
      // task.create 故意不回 chunk：模拟长任务挂起
    });
  });
  const port = (wss.address() as AddressInfo).port;
  const adapter = await WSAdapter.create(`ws://127.0.0.1:${port}`);
  try {
    const controller = new AbortController();
    // 不等 ack 先取消：ack 等待期被 abort 应立即放行（send 内静默 resolve），
    // 同时向 agent 补发 task.cancel
    const sendP = adapter.send({ task_id: "t9", type: "chat", content: "x" } as proto.LocalAgentRequest, controller.signal);
    controller.abort();
    await sendP;
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(cancelled, "t9");
  } finally {
    adapter.close();
    wss.close();
  }
});

// ---- ack 透传(task.create 拒绝响应不再丢失)----

// 起一个行为可控的最小 fake agent:onTask 自定义 task.create 的反应
function startAckAgent(onTask: (ws: import("ws").WebSocket, msg: proto.Message) => void):
    Promise<{ url: string; close: () => void }> {
  const wss = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  return new Promise((resolve) => wss.on("listening", () => {
    wss.on("connection", (ws) => {
      ws.on("message", (data) => {
        const msg = JSON.parse(data.toString()) as proto.Message;
        if (msg.method === proto.METHOD_LIFECYCLE_INITIALIZE) {
          ws.send(JSON.stringify({
            jsonrpc: "2.0", id: msg.id,
            result: { protocolVersion: "1.0.0", capabilities: {}, serverInfo: { name: "fake", version: "0" } },
          }));
          return;
        }
        if (msg.method === proto.METHOD_TASK_CREATE) onTask(ws, msg);
      });
    });
    const port = (wss.address() as AddressInfo).port;
    resolve({ url: `ws://127.0.0.1:${port}`, close: () => wss.close() });
  }));
}

test("WSAdapter：task.create 错误响应透传（拒绝不再丢失）", async () => {
  const fake = await startAckAgent((ws, msg) => {
    const p = msg.params as { task_id: string };
    ws.send(JSON.stringify({
      jsonrpc: "2.0", id: msg.id,
      error: { code: -32602, message: "workdir not found: /no/such/dir" },
    }));
    void p;
  });
  const adapter = await WSAdapter.create(fake.url);
  try {
    const controller = new AbortController();
    await assert.rejects(
      adapter.send({ task_id: "t1", type: "chat", content: "x" } as proto.LocalAgentRequest, controller.signal),
      (err: Error & { code?: number }) => err.code === -32602 && err.message.includes("/no/such/dir"),
    );
  } finally {
    adapter.close();
    fake.close();
  }
});

test("WSAdapter：无 ack 无输出 → 时限后超时拒绝", async () => {
  process.env.YWM_ACK_WAIT_MS = "150";
  try {
    const fake = await startAckAgent(() => { /* 故意既不 ack 也不出流 */ });
    const adapter = await WSAdapter.create(fake.url);
    try {
      const controller = new AbortController();
      await assert.rejects(
        adapter.send({ task_id: "t2", type: "chat", content: "x" } as proto.LocalAgentRequest, controller.signal),
        (err: Error & { code?: number }) => err.code === -32001,
      );
    } finally {
      adapter.close();
      fake.close();
    }
  } finally {
    delete process.env.YWM_ACK_WAIT_MS;
  }
});

test("WSAdapter：不回 ack 但先出流 → 隐式接受", async () => {
  const fake = await startAckAgent((ws, msg) => {
    const p = msg.params as { task_id: string };
    // 存量 agent 风格:不回 task.create 应答,直接推 chunk
    ws.send(JSON.stringify({
      jsonrpc: "2.0", method: "stream.chunk",
      params: { task_id: p.task_id, type: "text", content: [{ type: "text", text: "hi" }] },
    }));
    ws.send(JSON.stringify({
      jsonrpc: "2.0", method: "stream.chunk",
      params: { task_id: p.task_id, type: "text", done: true },
    }));
  });
  const adapter = await WSAdapter.create(fake.url);
  try {
    const controller = new AbortController();
    const chunks = await adapter.send({ task_id: "t3", type: "chat", content: "x" } as proto.LocalAgentRequest, controller.signal);
    const collected: proto.LocalAgentChunk[] = [];
    for await (const c of chunks) collected.push(c);
    assert.equal(collected.length, 2);
    assert.equal(collected[1].done, true);
  } finally {
    adapter.close();
    fake.close();
  }
});
