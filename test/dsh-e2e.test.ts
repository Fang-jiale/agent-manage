// dsh 真链路 E2E：真实 gateway + 真实 AgentClient（client.ts）×3 +
// 真实 dsh 管理者（local-agent-demo bin，编排配置）+ 两个假 worker 夹具。
// dsh 的模型端点指向本测试内起的 mock OpenAI SSE 服务（keyless，脚本化决策），
// 验证完整链路：群消息 @管理者 → dsh 决策调用 group_delegate → client 桥接
// agent.task.invoke → 网关鉴权/fan-out → worker 回复 → subtask_result 回投 →
// dsh 拿到结果出终答 → 归因落库 + run.list 可查。
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import { spawn, type ChildProcess } from "node:child_process";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { Db } from "../src/db.ts";
import { setLogLevel } from "../src/util.ts";
import { signJwt } from "../src/auth.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "dsh-e2e-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";
const OWNER = "u-dsh-e2e";
const DSH_ROOT = process.env.DSH_ROOT ?? "/Users/fangjiale/project/deepseek harness/deepseek-harness";

function jwtFor(sub: string): string {
  return signJwt({ sub, name: sub }, JWT_SECRET, 60_000);
}

function testConfig(): GatewayConfig {
  return {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 120_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 3_600_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "dsh-e2e",
    attachDir: "", attachQuotaMb: 0, retentionDays: 0,
    s3Endpoint: "", s3Region: "us-east-1", s3Bucket: "ywmatrix",
    s3AccessKey: "", s3SecretKey: "", s3PublicURL: "",
    oidcIssuer: "", oidcClientID: "", oidcClientSecret: "", oidcRedirectURL: "",
    oidcEmployeeClaim: "employee_id",
    trustProxy: false,
  };
}

class Conn {
  private ws: WebSocket;
  private buffer: proto.Message[] = [];
  private waiters: { method?: string; resolve: (m: proto.Message) => void; timer: NodeJS.Timeout }[] = [];

  private constructor(ws: WebSocket) {
    this.ws = ws;
    ws.on("message", (data) => {
      const msg = JSON.parse(data.toString()) as proto.Message;
      const idx = this.waiters.findIndex((w) => w.method === undefined || w.method === msg.method);
      if (idx !== -1) {
        const [w] = this.waiters.splice(idx, 1);
        clearTimeout(w.timer);
        w.resolve(msg);
      } else {
        this.buffer.push(msg);
      }
    });
  }

  static dial(url: string): Promise<Conn> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.once("open", () => resolve(new Conn(ws)));
      ws.once("error", reject);
    });
  }

  send(msg: proto.Message): void {
    this.ws.send(JSON.stringify(msg));
  }

  next(method?: string, timeoutMs = 5000): Promise<proto.Message> {
    const idx = this.buffer.findIndex((m) => method === undefined || m.method === method);
    if (idx !== -1) {
      const [m] = this.buffer.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.waiters.findIndex((w) => w.resolve === wrapped);
        if (i !== -1) this.waiters.splice(i, 1);
        reject(new Error(`timeout waiting for ${method ?? "message"}`));
      }, timeoutMs);
      const wrapped = (m: proto.Message): void => resolve(m);
      this.waiters.push({ method, resolve: wrapped, timer });
    });
  }

  close(): void {
    this.ws.close();
  }
}

async function rpc<T>(conn: Conn, id: string, method: string, params: object): Promise<T> {
  conn.send(proto.newRequest(id, method, params));
  for (;;) {
    const m = await conn.next(undefined, 30_000);
    if (m.id === id) return (m.error ? m : m.result) as T;
  }
}

// ---- mock OpenAI 兼容端点：脚本化管理者的两次模型调用，并记录请求体 ----
async function startMockLLM(w1: string, w2: string): Promise<{ server: http.Server; bodies: unknown[]; port: number }> {
  const bodies: unknown[] = [];
  let call = 0;
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      call += 1;
      try { bodies.push(JSON.parse(body)); } catch { /* 忽略 */ }
      res.setHeader("content-type", "text/event-stream");
      const delegateArgs = JSON.stringify({
        targets: [w1, w2],
        collect: "all",
        content: "调研两个方案的优劣并各自给出结论",
        invocation_id: "step-1",
        context_policy: "final_only",
      });
      const events = call === 1
        ? [
            { choices: [{ index: 0, delta: { role: "assistant", content: "我先委派两位成员分别调研。" } }] },
            { choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call-1", type: "function", function: { name: "group_delegate", arguments: delegateArgs } }] } }] },
            { choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
          ]
        : [
            { choices: [{ index: 0, delta: { role: "assistant", content: "两位成员的结论已收到，汇总完成。" } }] },
            { choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
          ];
      for (const e of events) res.write(`data: ${JSON.stringify(e)}\n\n`);
      res.write("data: [DONE]\n\n");
      res.end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  return { server, bodies, port };
}

function spawnClient(args: string[], env: Record<string, string>, cwd: string): ChildProcess {
  const clientEntry = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "src", "client.ts");
  const p = spawn(process.execPath, [clientEntry, ...args], {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["pipe", "pipe", "pipe"],
  });
  p.stderr?.on("data", () => { /* 测试自身断言失败面已足够定位 */ });
  return p;
}

async function waitFor(fn: () => Promise<boolean>, timeoutMs = 30_000, what = "condition"): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`waitFor timeout: ${what}`);
}

test("dsh manager delegates to group workers end-to-end", async (t) => {
  if (!fs.existsSync(path.join(DSH_ROOT, "packages/examples/local-agent-demo/lib/bin.js"))) {
    t.skip(`dsh 仓库不可用（${DSH_ROOT}），跳过 dsh E2E`);
    return;
  }
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过 dsh E2E");
    await db.close().catch(() => {});
    return;
  }
  const rid = crypto.randomUUID().slice(0, 8);
  const mgr = `dsh-${rid}-mgr`;
  const w1 = `dsh-${rid}-w1`;
  const w2 = `dsh-${rid}-w2`;
  const workdir = fs.mkdtempSync(path.join(os.tmpdir(), `dsh-e2e-${rid}-`));
  fs.writeFileSync(path.join(workdir, "hooks.json"), "{}");
  const { server } = await createGatewayServer(testConfig(), STATIC_FILE, db, undefined);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  const base = `ws://127.0.0.1:${port}`;

  const procs: ChildProcess[] = [];
  const userConns: Conn[] = [];
  let groupID = "";
  let llm: Awaited<ReturnType<typeof startMockLLM>> | undefined;
  try {
    llm = await startMockLLM(w1, w2);
    if (!(await db.getUserById(OWNER))) {
      await db.createUser({ id: OWNER, name: OWNER, password_hash: "x" });
    }
    for (const g of await db.listGroups(OWNER)) await db.deleteGroup(OWNER, g.id).catch(() => {});
    await db.deleteAgentsByOwner(OWNER).catch(() => {});

    // 三个 AgentClient：dsh 管理者 + 两个假 worker
    const dshBin = path.join(DSH_ROOT, "packages/examples/local-agent-demo/lib/bin.js");
    const dshCfg = path.join(DSH_ROOT, "packages/examples/local-agent/cordis.orchestration.yml");
    const mgrDir = path.join(workdir, "mgr");
    fs.mkdirSync(mgrDir, { recursive: true });
    procs.push(spawnClient(
      ["--gateway", `${base}/ws/agent`, "--agent-id", mgr, "--adapter", "stdio",
        "--local-agent", `${process.execPath} ${JSON.stringify(dshBin)} --config ${JSON.stringify(dshCfg)}`,
        "--token", jwtFor(OWNER), "--ui-addr", "127.0.0.1:0"],
      {
        DEEPSEEK_BASE_URL: `http://127.0.0.1:${llm!.port}/v1`,
        DEEPSEEK_API_KEY: "sk-e2e",
        DSH_TELEMETRY_DISABLED: "1",
        DSH_LOCAL_AGENT_SESSIONS_ROOT: path.join(mgrDir, ".sessions"),
        AGENT_MANAGE_LOG_LEVEL: "error",
      },
      mgrDir,
    ));
    for (const [id, reply] of [[w1, `${w1} 的调研结论A`], [w2, `${w2} 的调研结论B`]] as const) {
      const d = path.join(workdir, id);
      fs.mkdirSync(d, { recursive: true });
      procs.push(spawnClient(
        ["--gateway", `${base}/ws/agent`, "--agent-id", id, "--adapter", "stdio",
          "--local-agent", `${process.execPath} ${JSON.stringify(path.resolve("test/fixtures/fake-worker.mjs"))}`,
          "--token", jwtFor(OWNER), "--ui-addr", "127.0.0.1:0"],
        { FAKE_WORKER_ID: id, FAKE_WORKER_REPLY: reply, AGENT_MANAGE_LOG_LEVEL: "error" },
        d,
      ));
    }

    const userConn = await Conn.dial(`${base}/ws/admin?token=${jwtFor(OWNER)}`);
    userConns.push(userConn);
    await userConn.next(proto.METHOD_ADMIN_AGENT_LIST);

    // 等三个 agent 注册上线（DB 行就绪才能建群）
    await waitFor(async () => {
      const res = (await rpc<{ agents: Array<{ id: string; online: boolean }> }>(userConn, "al", proto.METHOD_AGENT_LIST, { limit: 100 })) ;
      return [mgr, w1, w2].every((id) => res.agents?.some((a) => a.id === id && a.online));
    }, 90_000, "三个 agent 上线");

    groupID = (await rpc<proto.GroupCreateResult>(userConn, "gc", proto.METHOD_GROUP_CREATE, {
      name: "dsh-e2e", agent_ids: [mgr, w1, w2], manager_agent_id: mgr,
    } satisfies proto.GroupCreateParams)).group_id;

    // 群消息 @管理者 → 全链路
    const parentTask = `${rid}-e2e`;
    const sessionID = crypto.randomUUID(); // 本地 Agent 标准要求 UUID 会话 id
    userConn.send(proto.newRequest("t1", proto.METHOD_TASK_CREATE, {
      group_id: groupID, task_id: parentTask, session_id: sessionID, type: "chat",
      content: "帮我调研两个方案", mentions: [mgr],
    } satisfies proto.TaskCreateParams));
    await userConn.next();

    // 探针：dsh 应发起首个 LLM 调用（否则断在 gateway→client→dsh 段）
    await waitFor(async () => (llm?.bodies.length ?? 0) >= 1, 45_000, "管理者首个 LLM 调用");

    // ① 两个 worker 的子任务进度带 parent_task_id 回到用户
    // 管理者自己的思考/文本 chunk 会先到：持续收帧直到两个 worker 的归因帧都出现
    const subtaskAgents = new Set<string>();
    const subtaskDeadline = Date.now() + 90_000;
    while (subtaskAgents.size < 2 && Date.now() < subtaskDeadline) {
      const p = proto.decodeParams<proto.AdminProgressParams>(
        await userConn.next(proto.METHOD_ADMIN_PROGRESS, 90_000));
      if (p.parent_task_id === parentTask) subtaskAgents.add(p.agent_id);
    }
    assert.deepEqual(subtaskAgents, new Set([w1, w2]), "两个 worker 的编排子任务都应归因到父任务");

    // ② 管理者终答：累计其文本帧直到 done（终态帧本身不带内容）
    let finalText = "";
    let mgrDone = false;
    await waitFor(async () => {
      for (;;) {
        try {
          const p = proto.decodeParams<proto.AdminProgressParams>(
            await userConn.next(proto.METHOD_ADMIN_PROGRESS, 1_000));
          if (p.agent_id === mgr) {
            finalText += (p.content ?? []).map((c) => c.text ?? "").join("");
            if (p.done) { mgrDone = true; return true; }
          }
        } catch {
          return mgrDone;
        }
      }
    }, 60_000, "管理者终答");
    assert.ok(finalText.includes("汇总完成"), `管理者终答应含汇总文本，实际: ${finalText}`);

    // ③ 第二次模型调用的请求体包含两个 worker 的结论（subtask_result 已回流进上下文）
    await waitFor(async () => (llm?.bodies.length ?? 0) >= 2);
    const secondBody = JSON.stringify(llm?.bodies[1]);
    assert.ok(secondBody.includes("调研结论A"), "第二次模型请求应含 worker1 结果");
    assert.ok(secondBody.includes("调研结论B"), "第二次模型请求应含 worker2 结果");
    // 第一次模型请求含群编排前言（成员名单注入）
    const firstBody = JSON.stringify(llm?.bodies[0]);
    assert.ok(firstBody.includes("群聊编排上下文"), `管理者首请求应含群上下文前言`);

    // ④ run.list：父任务下两条 completed 运行记录（invocation step-1）
    await waitFor(async () => {
      const res = (await rpc<proto.RunListResult>(userConn, "rl", proto.METHOD_RUN_LIST, {
        parent_task_id: parentTask,
      } satisfies proto.RunListParams));
      return res.runs.length === 2 && res.runs.every((r) => r.status === "completed");
    });
    const runs = (await rpc<proto.RunListResult>(userConn, "rl2", proto.METHOD_RUN_LIST, {
      parent_task_id: parentTask,
    } satisfies proto.RunListParams)).runs;
    assert.ok(runs.every((r) => r.invocation_id === "step-1"));
    assert.deepEqual(new Set(runs.map((r) => r.target_agent_id)), new Set([w1, w2]));

    // ⑤ 群会话落库：1 user + 2 worker assistant + 1 manager assistant
    const sessions = await db.listSessions(OWNER, `group:${groupID}`);
    assert.ok(sessions.some((s) => s.id === sessionID), "群会话应已落库");
    await waitFor(async () => (await db.countMessages(OWNER, sessionID)) >= 4);
    const msgs = await db.listMessages(OWNER, sessionID, 50);
    assert.equal(msgs.filter((m) => m.role === "user").length, 1);
    const assistants = msgs.filter((m) => m.role === "assistant");
    assert.deepEqual(new Set(assistants.map((m) => m.agent_id)), new Set([w1, w2, mgr]));
  } finally {
    for (const c of userConns) c.close();
    for (const p of procs) p.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 500));
    for (const p of procs) if (!p.killed) p.kill("SIGKILL");
    llm?.server.close();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    if (groupID) await db.deleteGroup(OWNER, groupID).catch(() => {});
    await db.deleteAgentsByOwner(OWNER).catch(() => {});
    await db.close();
    fs.rmSync(workdir, { recursive: true, force: true });
    void t;
  }
});
