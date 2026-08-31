// 用户管理 RPC（user.*）：admin 建号/禁用/重置密码/角色/删除、自助改密。
import * as proto from "../protocol.ts";
import type { Db, DbUser } from "../db.ts";
import { hashPassword, verifyPassword } from "../auth.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError } from "./hub.ts";
import type { UserConn } from "./types.ts";

export function userInfoOf(u: DbUser): proto.UserInfo {
  return { id: u.id, name: u.name, role: u.role, disabled: u.disabled === 1, created_at: u.created_at, last_login_at: u.last_login_at };
}

export async function requireAdmin(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<boolean> {
  if (!(await hub.isAdminUser(user.userID, db))) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "admin only");
    return false;
  }
  return true;
}

export async function handleUserList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserListParams>(msg);
  // 无分页参数时保持旧行为（全量），管理后台走分页路径
  if (params.query === undefined && params.limit === undefined && params.offset === undefined) {
    const users = await db.listUsers();
    sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
      users: users.map(userInfoOf),
      total: users.length,
    } satisfies proto.UserListResult));
    return;
  }
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const offset = Math.max(params.offset ?? 0, 0);
  const { users, total } = await db.listUsersPaged({ query: params.query || undefined, limit, offset });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    users: users.map(userInfoOf),
    total,
  } satisfies proto.UserListResult));
}

export async function handleUserSetRole(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserSetRoleParams>(msg);
  if (params.role !== "admin" && params.role !== "user") {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "role must be admin or user");
    return;
  }
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot change your own role");
    return;
  }
  if (!(await db.setUserRole(params.id, params.role))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.invalidateAdminCache(params.id);
  hub.kickUser(params.id); // isAdmin 缓存在连接上，强制重连刷新
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleUserCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserCreateParams>(msg);
  if (!params.name || !params.password) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name and password required");
    return;
  }
  if (await db.getUserByName(params.name)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name already taken");
    return;
  }
  let id: string = crypto.randomUUID();
  const manualID = (params.id || "").trim();
  if (manualID) {
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(manualID)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id must be 1-64 chars of A-Za-z0-9._-");
      return;
    }
    if (await db.getUserById(manualID)) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id already taken");
      return;
    }
    id = manualID;
  }
  const u: DbUser = {
    id,
    name: params.name,
    password_hash: await hashPassword(params.password),
    role: params.role === "admin" ? "admin" : "user",
    disabled: 0,
    created_at: Date.now(),
    last_login_at: null,
    employee_id: null,
    display_name: null,
  };
  await db.createUser(u);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", userInfoOf(u)));
}

export async function handleUserDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserDeleteParams>(msg);
  if (!params.id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id required");
    return;
  }
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot delete yourself");
    return;
  }
  if (!(await db.getUserById(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  const { agents } = await db.listAgentsPaged({ ownerID: params.id, limit: 100_000, offset: 0 });
  // 数据清理单事务原子完成（purge + agent 行 + 用户行）：此前三段分离的 await
  // 中途崩溃会留下"数据已清但账号还能登录"的半删除用户。hub 侧注销在事务成功后执行
  await db.deleteUserCompletely(params.id);
  for (const row of agents) {
    hub.unregisterAgent(row.id);
    await hub.pushConnectorSync(row.connector_id ?? "");
  }
  hub.invalidateAdminCache(params.id);
  hub.kickUser(params.id);
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleUserDisable(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserDisableParams>(msg);
  if (params.id === user.userID) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "cannot disable yourself");
    return;
  }
  if (!(await db.setUserDisabled(params.id, params.disabled))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.invalidateAdminCache(params.id);
  if (params.disabled) hub.kickUser(params.id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleUserResetPassword(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.UserResetPasswordParams>(msg);
  if (!params.id || !params.password) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "id and password required");
    return;
  }
  if (!(await db.setUserPassword(params.id, await hashPassword(params.password)))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "user not found");
    return;
  }
  hub.kickUser(params.id); // 强制重新登录
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleUserChangePassword(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.UserChangePasswordParams>(msg);
  const me = await db.getUserById(user.userID);
  if (!me || !(await verifyPassword(params.old_password ?? "", me.password_hash))) {
    sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "old password incorrect");
    return;
  }
  if (!params.new_password || params.new_password.length < 6) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "new password too short (>=6)");
    return;
  }
  await db.setUserPassword(me.id, await hashPassword(params.new_password));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 管理后台：agent 管理与概览 ----

