#!/usr/bin/env node
// wsecho：本地 Agent 的 WebSocket 形态演示服务（对齐《本地 Agent 接口标准》§2.3）。
// 用法：node scripts/local-agent-ws.mjs [-addr 127.0.0.1:9002]
import { WebSocketServer } from "ws";

const args = process.argv.slice(2);
const addrIdx = args.indexOf("-addr");
const addr = addrIdx !== -1 ? args[addrIdx + 1] : "127.0.0.1:9002";
const [host, port] = addr.split(":");

const CAPABILITIES = [
  { type: "chat", name: "coding", description: "编码助手（ws echo 演示）" },
  { type: "chat", name: "general", description: "通用对话能力" },
];

const wss = new WebSocketServer({ host, port: Number(port) }, () => {
  console.log(`[wsecho] listening on ${addr}`);
});

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function replyChunks(ws, req) {
  send(ws, { jsonrpc: "2.0", method: "stream.chunk", params: {
    task_id: req.task_id, type: "text",
    content: [{ type: "text", text: `[wsecho] 收到指令：${req.content ?? ""}` }],
  } });
  send(ws, { jsonrpc: "2.0", method: "stream.chunk", params: {
    task_id: req.task_id, type: "text", done: true,
    content: [{ type: "text", text: "演示回复完成。" }],
  } });
}

wss.on("connection", (ws) => {
  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    switch (msg.method) {
      case "lifecycle.initialize":
        send(ws, { jsonrpc: "2.0", id: msg.id, result: { capabilities: CAPABILITIES } });
        send(ws, { jsonrpc: "2.0", method: "lifecycle.register", params: {
          agent_id: "wsecho", name: "WS Echo 演示", version: "1.0.0",
          capabilities: CAPABILITIES,
          platform: { os: process.platform, arch: process.arch, hostname: "wsecho" },
        } });
        break;
      case "lifecycle.ping":
        send(ws, { jsonrpc: "2.0", id: msg.id, result: { pong: true } });
        break;
      case "task.create":
        send(ws, { jsonrpc: "2.0", id: msg.id, result: { status: "accepted", task_id: msg.params.task_id } });
        replyChunks(ws, msg.params);
        break;
      case "task.respond":
        send(ws, { jsonrpc: "2.0", id: msg.id, result: { status: "accepted", task_id: msg.params.task_id } });
        replyChunks(ws, msg.params);
        break;
      case "task.cancel": {
        const p = msg.params ?? {};
        send(ws, { jsonrpc: "2.0", method: "stream.chunk", params: {
          task_id: p.task_id, type: "text", done: true, reason: "cancel",
          content: [{ type: "text", text: "[wsecho] 任务已取消" }],
        } });
        break;
      }
      default:
        if (msg.id) send(ws, { jsonrpc: "2.0", id: msg.id, error: { code: -32601, message: "method not found" } });
    }
  });
});
