import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as proto from "./protocol.ts";
import { envString, parseFlags, logger, parseListenAddr } from "./util.ts";

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface Attachment {
  name: string;
  mime?: string;
  size?: number;
  data?: string;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / 1024 / 1024).toFixed(1) + " MB";
}

function composeReply(content: string, attachments: Attachment[], history: proto.ChatMessage[]): string {
  const lines: string[] = [];
  if (history.length > 0) {
    const prevUser = [...history].reverse().find(m => m.role === "user");
    lines.push(
      `这是我们本轮会话的第 **${Math.floor(history.length / 2) + 1}** 次对话。` +
      (prevUser ? `上一轮你说的是「${prevUser.content.slice(0, 30)}」。` : ""),
      "",
    );
  }
  if (content) {
    lines.push(`收到指令：**${content}**`, "");
  }
  if (attachments.length > 0) {
    lines.push(`同时收到 **${attachments.length} 个附件**：`, "");
    lines.push("| 文件 | 类型 | 大小 |", "| --- | --- | --- |");
    for (const a of attachments) {
      lines.push(`| ${a.name} | ${a.mime || "未知"} | ${formatSize(a.size ?? 0)} |`);
    }
    lines.push("");
  }
  lines.push(
    "已完成初步分析，结果如下：",
    "",
    "| 项目 | 状态 | 说明 |",
    "| --- | --- | --- |",
    `| 指令解析 | 完成 | 识别到 ${content.length} 个字符 |`,
    "| 任务调度 | 完成 | 已分配执行资源 |",
    "| 结果汇总 | 进行中 | 见下方代码 |",
    "",
    "```js",
    "// 示例：处理你的指令",
    "function handle(input) {",
    "  const result = analyze(input); // 解析并执行",
    '  return `done: ${result.summary}`;',
    "}",
    "```",
    "",
    "> 这是演示 Agent，输出用于展示控制台的 Markdown 渲染能力。",
    "",
    "可以继续试试：",
    "",
    "- 发送包含 `代码` 或 **加粗** 的指令",
    "- 让我输出 ~~删除线~~ 或 [链接](https://example.com)",
    "- 裸 URL 也会自动成链：https://example.com",
  );
  return lines.join("\n");
}

export function createLocalAgentServer() {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");

    if (url.pathname === "/capabilities") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        capabilities: [
          { type: "chat", name: "coding", description: "编码助手，可读写文件、执行命令、分析代码" },
          { type: "chat", name: "general", description: "通用对话能力" },
        ],
      }));
      return;
    }

    if (url.pathname === "/tasks") {
      if (req.method !== "POST") {
        res.writeHead(405).end("method not allowed");
        return;
      }

      let agentReq: proto.LocalAgentRequest;
      try {
        agentReq = JSON.parse(await readBody(req)) as proto.LocalAgentRequest;
      } catch (err) {
        res.writeHead(400).end(err instanceof Error ? err.message : String(err));
        return;
      }

      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      const attachments = (agentReq.metadata?.attachments as Attachment[] | undefined) ?? [];
      const history = (agentReq.metadata?.history as proto.ChatMessage[] | undefined) ?? agentReq.history ?? [];
      const reply = composeReply(agentReq.content ?? "", attachments, history);
      const STEP = 6;
      const total = Math.ceil(reply.length / STEP);

      for (let i = 0; i < total; i++) {
        const piece = reply.slice(i * STEP, (i + 1) * STEP);
        const chunk: proto.LocalAgentChunk = {
          type: proto.CHUNK_TYPE_TEXT,
          task_id: agentReq.task_id,
          session_id: agentReq.session_id,
          context_id: agentReq.context_id,
          content: proto.textContent(piece),
          done: i === total - 1,
        };
        chunk.percentage = ((i + 1) / total) * 100;
        const msg = proto.newNotification("stream.chunk", chunk);
        res.write(`data: ${JSON.stringify(msg)}\n\n`);
        await sleep(30);
      }
      res.end();
      return;
    }

    res.writeHead(404).end("not found");
  });
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const values = parseFlags([
    { name: "addr", type: "string", default: envString("AGENT_MANAGE_ADDR", ":9001") },
  ]);
  const { host, port } = parseListenAddr(values["addr"]);
  createLocalAgentServer().listen(port, host, () => {
    logger.info("local agent listening", { addr: values["addr"] });
  });
}
