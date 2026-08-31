// E2E 假 worker：最小本地 Agent 协议实现（stdio JSONL）。
// 环境变量：FAKE_WORKER_ID（注册自报 id）、FAKE_WORKER_REPLY（固定回复文本）。
// 行为：initialize 应答 → initialized 后注册 → task.create 应答 accepted →
// 一条文本 chunk + done chunk + task.completed。
import readline from "node:readline";

const reply = process.env.FAKE_WORKER_REPLY || "worker 结果";
const selfId = process.env.FAKE_WORKER_ID || "fake-worker";

const send = (m) => process.stdout.write(JSON.stringify(m) + "\n");

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  const trimmed = line.trim();
  if (trimmed === "") return;
  let m;
  try {
    m = JSON.parse(trimmed);
  } catch {
    return;
  }
  if (m.method === "lifecycle.initialize" && m.id) {
    send({
      jsonrpc: "2.0", id: m.id,
      result: {
        protocolVersion: "1.0.0",
        capabilities: { chat: {}, streaming: {} },
        serverInfo: { name: selfId, version: "0.0.1" },
      },
    });
  } else if (m.method === "lifecycle.initialized") {
    send({
      jsonrpc: "2.0", method: "lifecycle.register",
      params: {
        agent_id: selfId, name: selfId, version: "0.0.1",
        capabilities: [{ type: "chat", name: "research", description: "调研并给出结论" }],
        platform: { os: "darwin", arch: "arm64", hostname: "e2e-fake" },
      },
    });
  } else if (m.method === "task.create" && m.id) {
    const task = m.params?.task_id ?? "";
    send({ jsonrpc: "2.0", id: m.id, result: { task_id: task, status: "accepted" } });
    send({
      jsonrpc: "2.0", method: "stream.chunk",
      params: { task_id: task, type: "text", content: [{ type: "text", text: reply }], done: false },
    });
    send({
      jsonrpc: "2.0", method: "stream.chunk",
      params: { task_id: task, type: "text", content: [{ type: "text", text: "" }], done: true },
    });
    send({ jsonrpc: "2.0", method: "task.completed", params: { task_id: task, status: "completed" } });
  }
  // task.cancel / task.respond 等忽略（假 worker 即完即止）
});
