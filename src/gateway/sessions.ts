// 会话与消息 RPC（session.* / message.list）：列表/建/改名/绑定工作目录/删除/历史。
import * as proto from "../protocol.ts";
import { logger } from "../util.ts";
import type { Db } from "../db.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError } from "./hub.ts";
import type { UserConn } from "./types.ts";

export async function handleSessionList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionListParams>(msg);
  const sessions = await db.listSessions(user.userID, params.agent_id || undefined);
  const lastMsgs = await db.listLastMessages(user.userID);
  const previews = new Map(lastMsgs.map((m) => [m.session_id, storedPreview(m.role, m.content)]));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    sessions: sessions.map((s) => ({
      id: s.id,
      agent_id: s.agent_id,
      title: s.title,
      workdir: s.workdir ?? null,
      created_at: s.created_at,
      updated_at: s.updated_at,
      message_count: Number(s.message_count ?? 0),
      preview: previews.get(s.id) || "",
    })),
  } satisfies proto.SessionListResult));
}

// 从落库的消息 content JSON 里提取一句可展示的摘要（≤80 字）
export function storedPreview(role: string, content: string): string {
  try {
    const c = JSON.parse(content) as { text?: string; chunks?: { type?: string; text?: string; content?: unknown }[]; attachments?: { name?: string }[] };
    let text = "";
    if (role === "user") {
      text = c.text || "";
      if (!text && c.attachments?.length) text = "[附件] " + (c.attachments[0].name || "");
    } else {
      const t = (c.chunks || []).find((ch) => ch.type === "text");
      if (t) text = typeof t.text === "string" ? t.text : "";
      if (!text && c.chunks?.length) text = "[" + (c.chunks[c.chunks.length - 1].type || "chunk") + "]";
    }
    text = text.replace(/[#>*`\-]/g, " ").replace(/\s+/g, " ").trim();
    return text.length > 80 ? text.slice(0, 80) + "…" : text;
  } catch {
    return "";
  }
}

export async function handleSessionCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionCreateParams>(msg);
  if (!params.agent_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  const existing = params.id ? await db.getSession(user.userID, params.id) : undefined;
  if (existing) {
    sendMsg(user.ws, proto.newResponse(msg.id ?? "", sessionInfoOf(existing)));
    return;
  }
  // 目标归属校验：agent 型需本人可管理（查库，离线 agent 亦可建）；
  // 群组型需群主本人。否则任何用户都能往别人的 agent 下挂脏会话行。
  if (params.agent_id.startsWith("group:")) {
    if (!user.isAdmin && !(await db.getGroup(user.userID, params.agent_id.slice("group:".length)))) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not your group");
      return;
    }
  } else {
    const row = await db.getAgentRow(params.agent_id);
    if (!row || !hub.canManage(user, { ownerID: row.owner_id })) {
      sendError(user.ws, msg.id, proto.ERR_UNAUTHORIZED, "not authorized to create session for this agent");
      return;
    }
  }
  const workdir = params.workdir?.trim() || "";
  if (workdir.length > 512) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "workdir too long (max 512)");
    return;
  }
  const s = await db.createSession({
    id: params.id || crypto.randomUUID(),
    owner_id: user.userID,
    agent_id: params.agent_id,
    title: params.title?.trim() || "新会话",
    workdir: workdir || null,
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", sessionInfoOf(s)));
}

export function sessionInfoOf(s: { id: string; agent_id: string; title: string; workdir?: string | null; created_at: number; updated_at: number; message_count?: number }): proto.SessionInfo {
  return {
    id: s.id,
    agent_id: s.agent_id,
    title: s.title,
    workdir: s.workdir ?? null,
    created_at: s.created_at,
    updated_at: s.updated_at,
    message_count: Number(s.message_count ?? 0),
  };
}

export async function handleSessionSetWorkdir(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionSetWorkdirParams>(msg);
  const workdir = params.workdir.trim();
  if (workdir.length > 512) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "workdir too long (max 512)");
    return;
  }
  const ok = await db.setSessionWorkdir(user.userID, params.id, workdir === "" ? null : workdir);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", workdir: workdir === "" ? null : workdir }));
}

export async function handleSessionRename(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionRenameParams>(msg);
  const ok = await db.renameSession(user.userID, params.id, params.title);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 删除会话并级联清理消息里引用的附件文件（本地盘 / S3 通用）
export async function deleteSessionWithAttachments(hub: Hub, ownerID: string, sessionID: string): Promise<boolean> {
  const db = hub.db;
  if (!db) return false;
  const keys: string[] = [];
  if (hub.attachments) {
    let before: number | undefined;
    for (;;) {
      const rows = await db.listMessages(ownerID, sessionID, 500, before);
      if (rows.length === 0) break;
      for (const m of rows) {
        try {
          const c = JSON.parse(m.content) as { attachments?: { url?: string }[] };
          for (const a of c.attachments ?? []) {
            const k = a.url ? hub.attachments.keyFromUrl(a.url) : undefined;
            if (k) keys.push(k);
          }
        } catch { /* 非 JSON 内容跳过 */ }
      }
      if (rows.length < 500) break;
      before = rows[0].created_at;
    }
  }
  const ok = await db.deleteSession(ownerID, sessionID);
  if (ok && hub.attachments) {
    for (const k of keys) {
      await hub.attachments.delete(k)
        .catch((e) => logger.warn("attachment delete failed", { key: k, error: String(e) }));
    }
  }
  return ok;
}

export async function handleSessionDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.SessionDeleteParams>(msg);
  const ok = await deleteSessionWithAttachments(hub, user.userID, params.id);
  if (!ok) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleMessageList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.MessageListParams>(msg);
  const session = await db.getSession(user.userID, params.session_id);
  if (!session) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "session not found");
    return;
  }
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const [rows, total] = await Promise.all([
    db.listMessages(user.userID, params.session_id, limit, params.before),
    db.countMessages(user.userID, params.session_id),
  ]);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    messages: rows.map((m) => ({
      id: m.id,
      session_id: m.session_id,
      agent_id: m.agent_id,
      role: m.role,
      content: JSON.parse(m.content) as unknown,
      task_id: m.task_id,
      created_at: m.created_at,
    })),
    total,
  } satisfies proto.MessageListResult));
}

// ---- 群组（多 agent 会话）----

