import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import * as proto from "../src/protocol.ts";
import { HTTPAdapter } from "../src/adapters/http.ts";

test("HTTPAdapter send parses SSE stream", async () => {
  const chunks: proto.LocalAgentChunk[] = [
    { type: "text", task_id: "t1", content: proto.textContent("hello") },
    { type: "thinking", task_id: "t1", content: proto.textContent("reasoning") },
    { type: "text", task_id: "t1", content: proto.textContent(" world"), done: true },
  ];

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/capabilities") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ capabilities: [{ type: "chat", name: "general" }] }));
      return;
    }
    if (url.pathname === "/tasks") {
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      for (const chunk of chunks) {
        const msg = proto.newNotification("stream.chunk", chunk);
        res.write(`data: ${JSON.stringify(msg)}\n\n`);
      }
      res.end();
      return;
    }
    res.writeHead(404).end();
  });

  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;

  try {
    const adapter = await HTTPAdapter.create(`http://localhost:${port}`);
    assert.deepEqual(adapter.getCapabilities(), [{ type: "chat", name: "general" }]);

    const controller = new AbortController();
    const stream = await adapter.send(
      { task_id: "t1", type: "chat", content: "hi" },
      controller.signal,
    );

    const received: proto.LocalAgentChunk[] = [];
    for await (const chunk of stream) {
      received.push(chunk);
    }

    assert.equal(received.length, 3);
    assert.equal(received[0].type, "text");
    assert.equal(received[0].content?.[0].text, "hello");
    assert.equal(received[1].type, "thinking");
    assert.equal(received[2].done, true);
  } finally {
    server.close();
  }
});

test("HTTPAdapter fills missing ids from request", async () => {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/capabilities") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ capabilities: [] }));
      return;
    }
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    const chunk: proto.LocalAgentChunk = { type: "text", content: proto.textContent("x"), done: true };
    res.write(`data: ${JSON.stringify(proto.newNotification("stream.chunk", chunk))}\n\n`);
    res.end();
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;

  try {
    const adapter = await HTTPAdapter.create(`http://localhost:${port}`);
    const controller = new AbortController();
    const stream = await adapter.send(
      { task_id: "t-9", session_id: "s-9", context_id: "c-9", type: "chat" },
      controller.signal,
    );
    const received: proto.LocalAgentChunk[] = [];
    for await (const chunk of stream) received.push(chunk);

    assert.equal(received.length, 1);
    assert.equal(received[0].task_id, "t-9");
    assert.equal(received[0].session_id, "s-9");
    assert.equal(received[0].context_id, "c-9");
  } finally {
    server.close();
  }
});
