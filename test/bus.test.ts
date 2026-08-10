import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { Bus, type BusHandlers, type RegisteredAgent } from "../src/bus.ts";
import * as proto from "../src/protocol.ts";

const REDIS_URL = process.env.AGENT_MANAGE_TEST_REDIS_URL ?? "redis://localhost:6379";
// 与开发环境共用 Redis，用随机前缀隔离频道与键
const PREFIX = `ywm-test-${crypto.randomUUID().slice(0, 8)}`;

interface Recorder {
  agentMsgs: { agentID: string; msg: proto.Message }[];
  userMsgs: { ownerID: string; msg: proto.Message }[];
  pendings: { reqID: string; msg: proto.Message }[];
  agentsChanged: number;
}

function recorder(): { r: Recorder; handlers: BusHandlers } {
  const r: Recorder = { agentMsgs: [], userMsgs: [], pendings: [], agentsChanged: 0 };
  const handlers: BusHandlers = {
    onAgentMessage: (agentID, msg) => { r.agentMsgs.push({ agentID, msg }); },
    onUserMessage: (ownerID, msg) => { r.userMsgs.push({ ownerID, msg }); },
    onPendingResponse: (reqID, msg) => { r.pendings.push({ reqID, msg }); },
    onAgentsChanged: () => { r.agentsChanged++; },
  };
  return { r, handlers };
}

async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error("waitFor timeout");
}

async function newBus(t: import("node:test").TestContext, instanceID: string, handlers: BusHandlers): Promise<Bus | undefined> {
  const bus = new Bus(REDIS_URL, instanceID, 15_000, handlers, PREFIX);
  try {
    await bus.start();
    return bus;
  } catch {
    t.skip("Redis 不可用，跳过 bus 测试");
    await bus.stop().catch(() => {});
    return undefined;
  }
}

function agentOf(id: string, instanceID: string): RegisteredAgent {
  return {
    id,
    owner_id: "u-test",
    name: id,
    status: proto.AGENT_STATUS_ONLINE,
    capabilities: [{ type: "chat", name: "general" }],
    instance_id: instanceID,
    last_heartbeat: Date.now(),
  };
}

test("registry roundtrip", async (t) => {
  const bus = await newBus(t, "t-reg-1", recorder().handlers);
  if (!bus) return;
  const id = "t-" + crypto.randomUUID();
  try {
    await bus.registerAgent(agentOf(id, bus.instanceID));
    const got = await bus.getAgent(id);
    assert.equal(got?.instance_id, "t-reg-1");
    assert.ok((await bus.listAgents()).some((a) => a.id === id));

    const updated = { ...agentOf(id, bus.instanceID), status: proto.AGENT_STATUS_BUSY };
    await bus.refreshAgent(updated);
    assert.equal((await bus.getAgent(id))?.status, "busy");

    await bus.unregisterAgent(id);
    assert.equal(await bus.getAgent(id), undefined);
    assert.ok(!(await bus.listAgents()).some((a) => a.id === id));
  } finally {
    await bus.stop();
  }
});

test("cross-instance agent routing", async (t) => {
  const rec1 = recorder();
  const rec2 = recorder();
  const bus1 = await newBus(t, "t-route-1", rec1.handlers);
  if (!bus1) return;
  const bus2 = await newBus(t, "t-route-2", rec2.handlers);
  if (!bus2) { await bus1.stop(); return; }
  try {
    const msg = proto.newRequest("req-1", proto.METHOD_AGENT_CHAT, { task_id: "t1" });
    await bus1.sendToAgent("t-route-2", "agent-x", msg);
    await waitFor(() => rec2.r.agentMsgs.length === 1);
    assert.equal(rec2.r.agentMsgs[0].agentID, "agent-x");
    assert.equal(rec2.r.agentMsgs[0].msg.id, "req-1");
    // 发送方不应收到点对点消息
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(rec1.r.agentMsgs.length, 0);
  } finally {
    await bus1.stop();
    await bus2.stop();
  }
});

test("broadcast echo suppression", async (t) => {
  const rec1 = recorder();
  const rec2 = recorder();
  const bus1 = await newBus(t, "t-echo-1", rec1.handlers);
  if (!bus1) return;
  const bus2 = await newBus(t, "t-echo-2", rec2.handlers);
  if (!bus2) { await bus1.stop(); return; }
  try {
    const msg = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, { task_id: "t1" });
    await bus1.publishUserMessage("u-test", msg);
    await bus1.publishAgentsChanged();

    // 对端收到广播
    await waitFor(() => rec2.r.userMsgs.length === 1 && rec2.r.agentsChanged === 1)
    assert.equal(rec2.r.userMsgs[0].ownerID, "u-test");

    // 发送方被回声抑制，本地不重复投递
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(rec1.r.userMsgs.length, 0);
    assert.equal(rec1.r.agentsChanged, 0);
  } finally {
    await bus1.stop();
    await bus2.stop();
  }
});

test("pending response fans out to all instances", async (t) => {
  const rec1 = recorder();
  const rec2 = recorder();
  const bus1 = await newBus(t, "t-pend-1", rec1.handlers);
  if (!bus1) return;
  const bus2 = await newBus(t, "t-pend-2", rec2.handlers);
  if (!bus2) { await bus1.stop(); return; }
  try {
    const msg = proto.newResponse("req-9", { status: "accepted" });
    await bus2.publishPending("req-9", msg);
    // pending 包不做回声抑制：发布者本地查不到 pendingRequests 时是无害 no-op，
    // 持有该请求的实例必须收到
    await waitFor(() => rec1.r.pendings.length === 1 && rec2.r.pendings.length === 1);
    assert.equal(rec1.r.pendings[0].reqID, "req-9");
  } finally {
    await bus1.stop();
    await bus2.stop();
  }
});
