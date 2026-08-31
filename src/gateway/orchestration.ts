// 任务入口与管理者编排：task.create 路由（单 agent / 群 fan-out）、agent.task.invoke
// （幂等/线程/超时/策略/批量 collect）、群黑板 recent_turns 注入、run.list。
import crypto from "node:crypto";
import * as proto from "../protocol.ts";
import type { Db } from "../db.ts";
import { logger } from "../util.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError } from "./hub.ts";
import type { AgentConn, UserConn, TaskState, InvocationRecord } from "./types.ts";
import {
  MAX_GROUP_FANOUT, MAX_PARENT_SUBTASKS_RUNNING, MAX_PARENT_SUBTASKS_TOTAL,
  MAX_ORCHESTRATION_DEPTH, MAX_PARENT_INVOCATIONS,
  INVOKE_TIMEOUT_MIN_MS, INVOKE_TIMEOUT_MAX_MS,
  textOfChunk, finalOnlyChunks,
} from "./types.ts";

export async function handleTaskCreate(hub: Hub, user: UserConn, msg: proto.Message): Promise<void> {
  const params = proto.decodeParams<proto.TaskCreateParams>(msg);
  // 会话绑定的工作目录注入 metadata.workdir（单 agent / 群聊 / 编排子任务统一经此）；
  // 本地 Agent 自行决定如何使用，不识别则忽略，见 local-agent-interface §6.1
  if (hub.db) {
    const sess = await hub.db.getSession(user.userID, params.session_id || `${params.task_id}-session`).catch(() => undefined);
    if (sess?.workdir) {
      params.metadata = { ...(params.metadata ?? {}), workdir: sess.workdir };
    }
  }
  if (params.group_id) {
    await handleGroupTaskCreate(hub, user, msg, params);
    return;
  }
  const agent = await hub.resolveAgent(params.agent_id ?? "");
  if (!agent) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not found");
    return;
  }
  if (!hub.canManage(user, agent)) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to manage this agent");
    return;
  }
  // 待审批 agent 不接任务（仅本地连接可能处于 pending；注册表里的都已批准）
  if (hub.getAgent(params.agent_id ?? "")?.approval === "pending") {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "agent pending approval");
    return;
  }
  if (!hub.taskLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks, please slow down");
    return;
  }
  if (msg.id) hub.trackPendingRequest(msg.id, user, params.task_id);
  const sessionID = params.session_id || `${params.task_id}-session`;
  hub.trackTask(params.task_id, params.agent_id ?? "", user.userID, sessionID);
  hub.persistUserMessage(params, sessionID, user.userID);

  hub.forwardToAgent(params.agent_id ?? "", proto.newRequest(msg.id ?? "", proto.METHOD_AGENT_CHAT, {
    task_id: params.task_id,
    session_id: sessionID,
    context_id: params.context_id,
    type: params.type,
    content: params.content,
    metadata: params.metadata,
  } satisfies proto.AgentChatParams));
}

// ---- 群黑板（blackboard lite）：把会话里最近的"轮次"摘要注入 metadata.group.recent_turns ----
// 一轮 = 一条群用户消息 + 其后的各成员回复（fan-out 与编排子任务的落库消息都算）。
// 成员由此"看得见"群里发生过什么，群聊不再是互盲的并行私聊。
export const RECENT_TURN_COUNT = 3;         // 注入最近几轮
export const RECENT_TURN_MSG_SCAN = 60;     // 向后扫描的最近消息条数上限
export const RECENT_TURN_TOTAL_BYTES = 8192; // recent_turns 总体积上限（超出丢最旧的轮）
export const TURN_USER_TEXT_MAX = 200;
export const TURN_REPLY_TEXT_MAX = 300;

export function truncateText(s: string, max: number): string {
  return s.length > max ? s.slice(0, max) + "…" : s;
}

// 存储消息 content JSON → 可读文本（user 取 text；assistant 取 chunks 终态文本）
export function textOfStoredContent(role: string, content: string): string {
  try {
    const c = JSON.parse(content) as { text?: string; chunks?: proto.LocalAgentChunk[] };
    if (role === "user") return c.text ?? "";
    return finalOnlyChunks(c.chunks ?? []).map(textOfChunk).join("");
  } catch {
    return "";
  }
}

export async function recentTurnsOf(
  db: Db, ownerID: string, sessionID: string, nameOf: Map<string, string>, currentTurnID?: string,
): Promise<unknown[] | undefined> {
  let msgs;
  try {
    msgs = await db.listMessages(ownerID, sessionID, RECENT_TURN_MSG_SCAN);
  } catch {
    return undefined;
  }
  // 按用户消息切轮（正序）；当前轮（turn_id 命中）不注入——它的 user 消息就是本条 content
  const turns: Array<{ turn_id: string; user_text: string; replies: Array<{ agent_id: string; name: string; text: string }> }> = [];
  for (const m of msgs) {
    if (m.role === "user") {
      turns.push({ turn_id: m.task_id ?? m.id, user_text: truncateText(textOfStoredContent("user", m.content), TURN_USER_TEXT_MAX), replies: [] });
    } else if (m.role === "assistant" && turns.length > 0) {
      turns[turns.length - 1].replies.push({
        agent_id: m.agent_id,
        name: nameOf.get(m.agent_id) ?? m.agent_id,
        text: truncateText(textOfStoredContent("assistant", m.content), TURN_REPLY_TEXT_MAX),
      });
    }
  }
  let prev = turns.filter((t) => t.turn_id !== currentTurnID).slice(-RECENT_TURN_COUNT);
  // 体积收敛：超限从最旧的轮开始丢
  while (prev.length > 0 && JSON.stringify(prev).length > RECENT_TURN_TOTAL_BYTES) prev.shift();
  return prev.length > 0 ? prev : undefined;
}

// 群上下文注入：转发 agent.chat 时在 metadata.group 带上群/成员/管理者信息与最近轮次，
// 让本地 Agent 无需额外配置即可感知自己是否为管理者、群里有谁、本条 @ 了谁、之前聊了什么。
// metadata 是自由扩展字段，不识别的 Agent 自动忽略，无兼容性问题。
export async function buildGroupMetadata(
  hub: Hub, ownerID: string,
  group: { id: string; name: string; manager_agent_id: string | null; delegates?: string[] },
  members: string[], mentions: string[], base?: Record<string, unknown>,
  sessionID?: string, turnID?: string,
): Promise<Record<string, unknown>> {
  const meta: Record<string, unknown> = { ...(base ?? {}) };
  const profiles = await hub.agentProfilesOf(ownerID);
  const nameOf = new Map([...profiles].map(([id, p]) => [id, p.name]));
  const online = await hub.onlineAgentIDs();
  const g: Record<string, unknown> = {
    group_id: group.id,
    group_name: group.name,
    manager_agent_id: group.manager_agent_id,
    delegate_agent_ids: group.delegates ?? [],
    // 成员档案（Agent Card 内化）：管理者可按能力选目标，而不是靠用户口头介绍谁会什么
    members: members.map((id) => {
      const p = profiles.get(id);
      const caps = (p?.capabilities ?? []).slice(0, 8).map((c) => ({
        type: c.type, name: c.name,
        ...(c.description ? { description: c.description.slice(0, 200) } : {}),
      }));
      const firstDesc = (p?.capabilities ?? []).find((c) => c.description)?.description?.slice(0, 200);
      return {
        agent_id: id,
        name: p?.name ?? id,
        ...(firstDesc ? { description: firstDesc } : {}),
        ...(caps.length ? { capabilities: caps } : {}),
        online: online.has(id),
      };
    }),
    mentions,
  };
  if (turnID) g.turn_id = turnID;
  if (hub.db && sessionID) {
    const recent = await recentTurnsOf(hub.db, ownerID, sessionID, nameOf, turnID);
    if (recent) g.recent_turns = recent;
  }
  meta.group = g;
  return meta;
}

// 管理者 agent 编排：父任务的处理连接经 agent 通道调用群内另一 agent（子任务）。
// 鉴权：调用连接 === 承载父任务的连接 + 群管理者 === 父任务 agent + 目标同群且属主一致；
// depth 硬限 1（编排产生的子任务不能再发起编排）。多实例下任务态在创建实例，
// 跨实例 invoke 会得到 parent task not found（编排要求管理者与任务同实例）。
// 可选参数：invocation_id（幂等，命中既有调用不重复派发）、targets[]（批量派发，
// 同 invocation_id 一组）、collect（all 默认 / first / quorum(n)：条件满足收割其余）、
// thread_id（续聊线程，批量时按目标各建线程）、timeout_ms（本次子任务超时覆盖）、
// context_policy（final_only 默认 / full）。
// 同父任务未决子任务并发 ≤ MAX_PARENT_SUBTASKS_RUNNING。
export async function handleAgentTaskInvoke(
  hub: Hub, agent: AgentConn, msg: proto.Message, params: proto.AgentTaskInvokeParams,
): Promise<void> {
  if (!hub.db) {
    sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  const ts = hub.tasks.get(params.parent_task_id);
  if (!ts) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "parent task not found");
    return;
  }
  if (ts.depth >= MAX_ORCHESTRATION_DEPTH) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION,
      `orchestration depth limit exceeded (max ${MAX_ORCHESTRATION_DEPTH})`);
    return;
  }
  const parentConn = hub.agents.get(ts.agentID);
  if (!parentConn || parentConn.ws !== agent.ws) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "parent task not served by this connection");
    return;
  }
  const db = hub.db;
  const group = await db.getGroup(ts.ownerID, params.group_id);
  // 鉴权（权限矩阵）：群管理者 ∪ 群授权 delegates 可发起编排
  const delegates = group ? await db.listGroupDelegates(group.id) : [];
  if (!group || (group.manager_agent_id !== ts.agentID && !delegates.includes(ts.agentID))) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "caller is not authorized to orchestrate (manager or delegate)");
    return;
  }
  const members = await db.listGroupMembers(group.id);
  // 目标解析：targets[]（批量，去重）或 target_agent_id（单目标），二选一
  const targets = params.targets?.length
    ? [...new Set(params.targets.filter((t) => t !== ""))]
    : [params.target_agent_id].filter((t) => t !== "");
  if (targets.length === 0) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "target required (target_agent_id or targets)");
    return;
  }
  if (targets.length > MAX_GROUP_FANOUT) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, `too many targets (max ${MAX_GROUP_FANOUT})`);
    return;
  }
  for (const t of targets) {
    if (t === ts.agentID || !members.includes(t)) {
      sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, "target agent not in group");
      return;
    }
  }
  // 收集策略：all（默认等全部）/ first（首个成功收割其余）/ quorum(n)（n 个成功收割其余）
  let collect: "all" | "first" | { quorum: number } = "all";
  if (params.collect === "first") {
    collect = "first";
  } else if (params.collect !== undefined && params.collect !== "all" && typeof params.collect === "object") {
    const q = Math.floor(params.collect.quorum);
    if (!Number.isFinite(q) || q < 1 || q > targets.length) {
      sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, `quorum must be 1..${targets.length}`);
      return;
    }
    collect = { quorum: q };
  }
  const invocationID = params.invocation_id?.trim().slice(0, 128) ?? "";
  // 幂等：同 parent+invocation_id 命中既有记录——仍在跑返回同子任务；已结束重发终态结果。
  // 要重跑请换新 invocation_id。
  if (invocationID !== "") {
    const record = ts.invocations?.get(invocationID);
    if (record) {
      const running = record.children.filter((c) => !c.done);
      for (const c of record.children) {
        if (c.lastResult && running.length === 0) {
          // 接收方 = 父任务的承载 agent（管理者）；父任务自身没有 invokerAgentID（那是子任务字段）
          hub.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_TASK_RESULT, c.lastResult));
        }
      }
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", {
        task_id: (running[0] ?? record.children[record.children.length - 1]).taskID,
        status: running.length > 0 ? "dispatched" : "duplicate",
        context_policy: params.context_policy === "full" ? "full" : "final_only",
        tasks: record.children.map((c) => ({ target_agent_id: c.target, task_id: c.taskID })),
      } satisfies proto.AgentTaskInvokeResult));
      return;
    }
  }
  // 每父任务并发上限：管理者连发会把目标 agent 打满（限速器按属主记，兜不住单任务）
  let running = 0;
  for (const t of hub.tasks.values()) {
    if (t.parentTaskID === params.parent_task_id) running++;
  }
  if (running + targets.length > MAX_PARENT_SUBTASKS_RUNNING) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION,
      `too many running subtasks (max ${MAX_PARENT_SUBTASKS_RUNNING})`);
    return;
  }
  // 预算制：本父任务累计派发的子任务总数（含已完成）也受限——多级/重试都不能刷出无限任务
  if ((ts.subtasksDispatched ?? 0) + targets.length > MAX_PARENT_SUBTASKS_TOTAL) {
    sendError(agent.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION,
      `subtask budget exceeded (max ${MAX_PARENT_SUBTASKS_TOTAL} per parent task)`);
    return;
  }
  ts.subtasksDispatched = (ts.subtasksDispatched ?? 0) + targets.length;
  for (const t of targets) {
    const target = await hub.resolveAgent(t);
    if (!target || target.ownerID !== ts.ownerID) {
      sendError(agent.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, `target agent not found: ${t}`);
      return;
    }
    if (hub.getAgent(t)?.approval === "pending") {
      sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, `target agent pending approval: ${t}`);
      return;
    }
  }
  if (!hub.taskLimiter.allow(ts.ownerID)) {
    sendError(agent.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks");
    return;
  }
  // 续聊线程：同 (group, invoker, target, thread_id) 复用稳定子会话 id（目标 agent 侧上下文连续）。
  // 子会话不落库——进度归因仍走群/父会话（progress 处理优先取网关侧登记的 sessionID）
  const threadSessions = new Map<string, string>();
  const threadID = params.thread_id?.trim().slice(0, 128) ?? "";
  if (threadID !== "") {
    for (const t of targets) {
      const key = `${group.id}:${ts.agentID}:${t}:${threadID}`;
      let sid = hub.invocationThreads.get(key);
      if (!sid) {
        sid = crypto.randomUUID();
        hub.invocationThreads.set(key, sid);
      }
      threadSessions.set(t, sid);
    }
  }
  // invocation 记录先于派发登记：终态回投（notifySubtaskResult）据此留存/更新与 collect 收割
  let record: InvocationRecord | undefined;
  if (invocationID !== "") {
    let m = ts.invocations;
    if (!m) {
      m = new Map();
      ts.invocations = m;
    }
    if (m.size >= MAX_PARENT_INVOCATIONS) m.delete(m.keys().next().value as string); // FIFO 淘汰
    record = { invocationID, children: [], collect, collected: false };
    m.set(invocationID, record);
  }
  const timeoutMs = params.timeout_ms !== undefined
    ? Math.min(Math.max(Math.floor(params.timeout_ms), INVOKE_TIMEOUT_MIN_MS), INVOKE_TIMEOUT_MAX_MS)
    : undefined;
  const policy: "final_only" | "full" = params.context_policy === "full" ? "full" : "final_only";
  const childMeta = await buildGroupMetadata(
    hub, ts.ownerID, { ...group, delegates }, members, targets, params.metadata, ts.sessionID, params.parent_task_id);
  // 子任务复用父任务会话：会话绑定的 workdir 同样注入（manager 未显式携带时兜底）
  const childSession = ts.sessionID
    ? await hub.db!.getSession(ts.ownerID, ts.sessionID).catch(() => undefined)
    : undefined;
  if (childSession?.workdir && childMeta.workdir === undefined) {
    childMeta.workdir = childSession.workdir;
  }
  const tasks: Array<{ target_agent_id: string; task_id: string; thread_session_id?: string }> = [];
  for (const t of targets) {
    const childTaskID = `${params.parent_task_id}@${crypto.randomUUID().slice(0, 8)}`;
    const threadSessionID = threadSessions.get(t);
    hub.trackTask(childTaskID, t, ts.ownerID, ts.sessionID, {
      groupID: group.id,
      parentTaskID: params.parent_task_id,
      invokerAgentID: ts.agentID,
      depth: ts.depth + 1,
      contextPolicy: policy,
      invocationID: invocationID !== "" ? invocationID : undefined,
      threadSessionID,
      ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    });
    if (record) record.children.push({ taskID: childTaskID, target: t, done: false, ok: false });
    hub.recordRunStart(childTaskID, hub.tasks.get(childTaskID)!);
    tasks.push({ target_agent_id: t, task_id: childTaskID, ...(threadSessionID ? { thread_session_id: threadSessionID } : {}) });
    hub.forwardToAgent(t, proto.newRequest("", proto.METHOD_AGENT_CHAT, {
      task_id: childTaskID,
      session_id: threadSessionID ?? ts.sessionID, // thread 模式下目标 agent 见稳定子会话
      type: params.type,
      content: params.content,
      metadata: childMeta,
    } satisfies proto.AgentChatParams));
  }
  sendMsg(agent.ws, proto.newResponse(msg.id ?? "", {
    task_id: tasks[0].task_id,
    status: "dispatched",
    context_policy: policy,
    tasks,
    ...(threadSessions.size === 1 ? { thread_session_id: [...threadSessions.values()][0] } : {}),
  } satisfies proto.AgentTaskInvokeResult));
}

// 群聊路径：@提及 路由（mentions 空则拒绝，保证"默认不触发"），多目标 fan-out 派生 task_id（<tid>#<n>）。
// 网关立即应答 task.create（不等 agent），避免多个 agent 对同一 msg.id 重复响应。
export async function handleGroupTaskCreate(
  hub: Hub, user: UserConn, msg: proto.Message, params: proto.TaskCreateParams,
): Promise<void> {
  if (!hub.db) {
    sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage not configured");
    return;
  }
  const db = hub.db;
  const group = await db.getGroup(user.userID, params.group_id!);
  if (!group) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  const members = await db.listGroupMembers(group.id);
  const mentions = [...new Set(params.mentions ?? [])];
  if (mentions.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "mentions required in group chat (@agent or @all)");
    return;
  }
  const targets = mentions.includes("all") ? members : mentions;
  if (targets.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group has no member agents");
    return;
  }
  if (targets.length > MAX_GROUP_FANOUT) {
    sendError(user.ws, msg.id, proto.ERR_ORCHESTRATION_VIOLATION, `too many targets (max ${MAX_GROUP_FANOUT})`);
    return;
  }
  for (const m of targets) {
    if (!members.includes(m)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `agent not in group: ${m}`);
      return;
    }
  }
  if (!hub.taskLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many tasks, please slow down");
    return;
  }
  // 离线目标不派发（forwardToAgent 只会静默丢弃）：跳过并在响应中告知
  const online: string[] = [];
  const skipped: string[] = [];
  for (const t of targets) {
    if (await hub.resolveAgent(t)) online.push(t);
    else skipped.push(t);
  }
  if (online.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, `all mentioned agents offline: ${skipped.join(", ")}`);
    return;
  }
  const sessionID = params.session_id || `${params.task_id}-session`;
  // 群会话标识 group:<gid>；一条 user 消息只落一次
  hub.persistUserMessage({ ...params, agent_id: `group:${group.id}` }, sessionID, user.userID);

  const taskIDs: string[] = [];
  const groupMeta = await buildGroupMetadata(
    hub, user.userID, { ...group, delegates: await db.listGroupDelegates(group.id) },
    members, online, params.metadata, sessionID, params.task_id);
  online.forEach((target, i) => {
    const taskID = online.length === 1 ? params.task_id : `${params.task_id}#${i}`;
    taskIDs.push(taskID);
    hub.trackTask(taskID, target, user.userID, sessionID, { groupID: group.id });
    hub.forwardToAgent(target, proto.newRequest("", proto.METHOD_AGENT_CHAT, {
      task_id: taskID,
      session_id: sessionID,
      context_id: params.context_id,
      type: params.type,
      content: params.content,
      metadata: groupMeta,
    } satisfies proto.AgentChatParams));
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    task_id: params.task_id,
    status: "accepted",
    group_id: group.id,
    task_ids: taskIDs,
    ...(skipped.length ? { skipped_offline: skipped } : {}),
  }));
}

// 编排子任务运行记录查询：owner 范围（admin 可看全部），供前端派发树/审计
export async function handleRunList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.RunListParams>(msg);
  const runs = await db.listRuns({
    ownerID: user.isAdmin ? undefined : user.userID,
    parentTaskID: params.parent_task_id || undefined,
    sessionID: params.session_id || undefined,
    limit: Math.min(Math.max(Math.floor(params.limit ?? 200), 1), 500),
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    runs: runs.map((r) => ({
      task_id: r.id,
      parent_task_id: r.parent_task_id,
      group_id: r.group_id,
      invoker_agent_id: r.invoker_agent_id,
      target_agent_id: r.target_agent_id,
      invocation_id: r.invocation_id,
      session_id: r.session_id,
      status: r.status,
      error: r.error,
      created_at: r.created_at,
      ended_at: r.ended_at,
    })),
  } satisfies proto.RunListResult));
}

// 群级取消：按基任务 id 收敛 fan-out 派生任务（<tid>#n）与编排子任务（parent 指向本批），
// 逐个下发 agent.cancel；任务清理仍由 agent 的 done 进度驱动（与单 agent 取消一致）
export function handleGroupTaskCancel(hub: Hub, user: UserConn, msg: proto.Message, params: proto.TaskCancelParams): void {
  const prefix = `${params.task_id}#`;
  const matches: Array<[string, TaskState]> = [];
  for (const [tid, ts] of hub.tasks) {
    const inFamily = tid === params.task_id || tid.startsWith(prefix)
      || (ts.parentTaskID !== undefined && (ts.parentTaskID === params.task_id || ts.parentTaskID.startsWith(prefix)));
    if (inFamily && (ts.ownerID === user.userID || user.isAdmin)) matches.push([tid, ts]);
  }
  for (const [tid, ts] of matches) {
    hub.finishRun(tid, "cancelled", "user cancelled");
    hub.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
      task_id: tid,
      session_id: ts.threadSessionID || ts.sessionID || undefined,
    } satisfies proto.AgentCancelParams));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { task_id: params.task_id, status: "cancelling" } satisfies proto.TaskCancelResult));
}

// cancel/respond 的公共转发逻辑：agent_id 为空时广播给该用户名下所有 agent
export async function handleTaskForward(
  hub: Hub, user: UserConn, msg: proto.Message, method: string,
  params: object, agentID?: string,
): Promise<void> {
  // 登记 pendingRequest，让 client.ts 的响应能通过 forwardToPendingUser 路由回 browser。
  // task.cancel 通常不期待有意义的响应，但 task.respond 必须把 result 透传回去（rule ③）。
  if (msg.id) hub.trackPendingRequest(msg.id, user);
  const req = proto.newRequest(msg.id ?? "", method, params);
  const target = agentID ?? (params as { agent_id?: string }).agent_id ?? "";
  if (target !== "") {
    const agent = await hub.resolveAgent(target);
    if (!agent) {
      sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not found");
      return;
    }
    if (!hub.canManage(user, agent)) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to manage this agent");
      return;
    }
    hub.forwardToAgent(target, req);
    return;
  }
  for (const id of await hub.resolveOwnerAgentIDs(user.userID)) {
    hub.forwardToAgent(id, req);
  }
}

