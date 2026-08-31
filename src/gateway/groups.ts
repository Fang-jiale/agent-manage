// 群组管理 RPC（group.*）：CRUD、管理者与 delegates 授权矩阵、群变更级联取消。
import * as proto from "../protocol.ts";
import type { Db } from "../db.ts";
import { logger } from "../util.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError } from "./hub.ts";
import type { UserConn } from "./types.ts";
import { MAX_GROUP_FANOUT } from "./types.ts";

export function groupInfoOf(
  g: { id: string; name: string; manager_agent_id: string | null; created_at: number },
  agentIDs: string[], delegateIDs: string[] = [],
): proto.GroupInfo {
  return {
    id: g.id, name: g.name, manager_agent_id: g.manager_agent_id,
    delegate_agent_ids: delegateIDs, agent_ids: agentIDs, created_at: g.created_at,
  };
}

// 校验 agent 归属：admin 可用他人 agent，普通用户仅自己的
export async function requireOwnedAgent(db: Db, user: UserConn, agentID: string): Promise<boolean> {
  const row = await db.getAgentRow(agentID);
  return !!row && (user.isAdmin || row.owner_id === user.userID);
}

// 备注名仅属主可设（昵称按 owner 私有，只影响自己的显示）
export async function handleAgentSetNickname(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentSetNicknameParams>(msg);
  if (!params.agent_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  if (!(await requireOwnedAgent(db, user, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not available");
    return;
  }
  const nickname = params.nickname?.trim().slice(0, 256) ?? "";
  await db.setNickname(user.userID, params.agent_id, nickname === "" ? null : nickname);
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    status: "ok",
    nickname: nickname === "" ? null : nickname,
  } satisfies proto.AgentSetNicknameResult));
}

export async function handleGroupCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {  const params = proto.decodeParams<proto.GroupCreateParams>(msg);
  const agentIDs = [...new Set(params.agent_ids ?? [])];
  if (!params.name?.trim()) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (agentIDs.length === 0) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_ids required");
    return;
  }
  for (const id of agentIDs) {
    if (!(await requireOwnedAgent(db, user, id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `agent not available: ${id}`);
      return;
    }
  }
  if (params.manager_agent_id && !agentIDs.includes(params.manager_agent_id)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "manager_agent_id must be a group member");
    return;
  }
  const groupID = crypto.randomUUID();
  const g = await db.createGroup({
    id: groupID,
    owner_id: user.userID,
    name: params.name.trim().slice(0, 128),
    manager_agent_id: params.manager_agent_id ?? null,
  });
  for (const id of agentIDs) await db.addGroupMember(groupID, id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { group_id: groupID } satisfies proto.GroupCreateResult));
}

export async function handleGroupList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const groups = await db.listGroups(user.userID);
  const infos: proto.GroupInfo[] = [];
  for (const g of groups) {
    infos.push(groupInfoOf(g, await db.listGroupMembers(g.id), await db.listGroupDelegates(g.id)));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { groups: infos } satisfies proto.GroupListResult));
}

export async function handleGroupDetail(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupDetailParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    group: groupInfoOf(g, await db.listGroupMembers(g.id), await db.listGroupDelegates(g.id)),
  } satisfies proto.GroupDetailResult));
}

export async function handleGroupAdd(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupAddParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (!(await requireOwnedAgent(db, user, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not available");
    return;
  }
  await db.addGroupMember(params.group_id, params.agent_id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleGroupRemove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupRemoveParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (!(await db.removeGroupMember(params.group_id, params.agent_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not in group");
    return;
  }
  if (g.manager_agent_id === params.agent_id) await db.setGroupManager(user.userID, params.group_id, null);
  await db.removeGroupDelegate(params.group_id, params.agent_id); // 出群同步清掉授权
  // 成员已出群：该成员名下未完成的群任务级联取消（后续 invoke 也会因非成员被拒）
  cancelGroupTasks(hub, params.group_id, params.agent_id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleGroupRename(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupRenameParams>(msg);
  const name = (params.name || "").trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (!(await db.getGroup(user.userID, params.group_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  await db.renameGroup(user.userID, params.group_id, name.slice(0, 128));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleGroupSetManager(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupSetManagerParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  if (params.manager_agent_id) {
    const members = await db.listGroupMembers(g.id);
    if (!members.includes(params.manager_agent_id)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "manager_agent_id must be a group member");
      return;
    }
  }
  await db.setGroupManager(user.userID, params.group_id, params.manager_agent_id ?? null);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 授权矩阵：整组替换可发起编排的成员（须为群成员）。管理者之外的授权成员 =
// delegate，能像管理者一样 agent.task.invoke（受同样的深度/预算/并发约束）
export async function handleGroupSetDelegates(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupSetDelegatesParams>(msg);
  const g = await db.getGroup(user.userID, params.group_id);
  if (!g) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  const ids = [...new Set(params.agent_ids ?? [])];
  if (ids.length > MAX_GROUP_FANOUT) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `too many delegates (max ${MAX_GROUP_FANOUT})`);
    return;
  }
  const members = await db.listGroupMembers(g.id);
  for (const id of ids) {
    if (!members.includes(id)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, `delegate must be a group member: ${id}`);
      return;
    }
  }
  await db.setGroupDelegates(g.id, ids);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", delegate_agent_ids: ids }));
}

// 群删除/成员移除后：取消该群未完成任务（agentID 给定则只取消该成员的）。
// 与 handleGroupTaskCancel 一致：只下发 agent.cancel，任务清理由 agent 的 done 进度驱动
export function cancelGroupTasks(hub: Hub, groupID: string, agentID?: string): void {
  for (const [tid, ts] of hub.tasks) {
    if (ts.groupID !== groupID) continue;
    if (agentID !== undefined && ts.agentID !== agentID) continue;
    logger.info("cancel task after group change", { task_id: tid, group_id: groupID, agent_id: ts.agentID });
    hub.finishRun(tid, "cancelled", "group changed");
    hub.forwardToAgent(ts.agentID, proto.newNotification(proto.METHOD_AGENT_CANCEL, {
      task_id: tid,
      session_id: ts.threadSessionID || ts.sessionID || undefined,
    } satisfies proto.AgentCancelParams));
  }
}

export async function handleGroupDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.GroupDeleteParams>(msg);
  if (!(await db.deleteGroup(user.userID, params.group_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "group not found");
    return;
  }
  // 群已解散：全部未完成群任务级联取消（fan-out 家族与编排子任务都带 groupID）
  cancelGroupTasks(hub, params.group_id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 用户管理 ----

