// 消息分发：agent 通道（handleAgentMessage）与用户通道（handleUserMessage）的
// method → handler 路由表。只做分发与公共校验，业务在各 handler 模块。
import * as proto from "../protocol.ts";
import { logger } from "../util.ts";
import type { Db } from "../db.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError, withDb } from "./hub.ts";
import type { AgentConn, UserConn } from "./types.ts";
import {
  handleSessionList, handleSessionCreate, handleSessionRename, handleSessionSetWorkdir,
  handleSessionDelete, handleMessageList,
} from "./sessions.ts";
import {
  handleGroupCreate, handleGroupList, handleGroupDetail, handleGroupAdd, handleGroupRemove,
  handleGroupRename, handleGroupSetManager, handleGroupSetDelegates, handleGroupDelete,
  handleAgentSetNickname,
} from "./groups.ts";
import { handleGroupRun } from "./orchestration.ts";
import {
  handleUserList, handleUserSetRole, handleUserCreate, handleUserDelete,
  handleUserDisable, handleUserResetPassword, handleUserChangePassword,
} from "./users.ts";
import {
  handleAgentList, handleAgentDisconnect, handleAgentReassign, handleAdminOverview,
  handleDeviceKeyCreate, handleDeviceKeyList, handleDeviceKeyRevoke,
  handlePairingCreate, handlePairingList, handlePairingDelete,
  handleConnectorPendingList, handleConnectorApprove, handleConnectorReject,
  handleConnectorPair, handleBrandList, handleBrandCreate, handleBrandUpdate,
  handleBrandDelete, handleProductPush, handleAgentApprove, handleAgentReject,
  handleConnectorList, handleAgentAssign, handleAgentRemove, handleAgentRestart,
  handleAgentRegister, brandInfoOf, doAssignAgent, doRemoveAgent,
} from "./agents.ts";
import {
  handleTaskCreate, handleGroupTaskCancel, handleRunList,
  handleAgentTaskInvoke, handleTaskForward,
} from "./orchestration.ts";

export function handleAgentMessage(hub: Hub, agent: AgentConn, raw: string): void {
  let msg: proto.Message;
  try {
    msg = JSON.parse(raw) as proto.Message;
  } catch {
    sendError(agent.ws, "", proto.ERR_PARSE_ERROR, "parse error");
    return;
  }

  if (msg.jsonrpc !== proto.VERSION) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "invalid jsonrpc version");
    return;
  }

  // 配对连接（?pair=1，无凭证）只允许 connector.pair
  if (agent.pairing && msg.method !== proto.METHOD_CONNECTOR_PAIR) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "pairing connection: only connector.pair allowed");
    return;
  }

  switch (msg.method) {
    case proto.METHOD_CONNECTOR_PAIR: {
      if (!agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_INVALID_REQUEST, "connector.pair requires a ?pair=1 connection");
        break;
      }
      void handleConnectorPair(hub, agent, msg).catch((e) => {
        logger.error("connector pair failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_REGISTER: {
      const params = proto.decodeParams<proto.RegisterParams>(msg);
      void handleAgentRegister(hub, agent, params, msg).catch((e) => {
        logger.error("register failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_CONNECTOR_HELLO: {
      const params = proto.decodeParams<proto.ConnectorHelloParams>(msg);
      if (!params.connector_id) {
        sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "connector_id required");
        break;
      }
      // 同 connector_id 已在其他连接上报到（双实例并存）：踢掉旧连接，
      // 保证同一时刻只有一个实例承载该 connector 的 agent
      const prevConn = hub.connectors.get(params.connector_id);
      if (prevConn && prevConn.ws !== agent.ws) {
        logger.warn("connector re-hello on new connection, kicking old", { connector_id: params.connector_id });
        prevConn.ws.close(4002, "replaced by new connection");
      }
      agent.connectorID = params.connector_id;
      if (params.platform) agent.platform = params.platform;
      hub.registerConnector(agent);
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
      // 连接建立即推全量目标集（重连后自动恢复承载的 agent）
      void hub.pushConnectorSync(params.connector_id)
        .catch((e) => logger.error("connector sync failed", { error: String(e) }));
      break;
    }

    // ---- connector 自助（client 本地管理页经 agent 通道调用）：仅限本 connector ----

    case proto.METHOD_BRAND_LIST: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      const brands = [...hub.brands.values()].filter((b) => b.disabled !== 1).map(brandInfoOf);
      sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { brands } satisfies proto.BrandListResult));
      break;
    }

    case proto.METHOD_AGENT_ASSIGN: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      if (!hub.db) {
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
        break;
      }
      const params = proto.decodeParams<proto.AgentAssignParams>(msg);
      if (params.connector_id !== agent.connectorID) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "cannot assign to another connector");
        break;
      }
      const db = hub.db;
      void doAssignAgent(hub, db, agent, params).then((r) => {
        if ("error" in r) {
          sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, r.error);
        } else {
          sendMsg(agent.ws, proto.newResponse(msg.id ?? "", {
            agent_id: r.agent_id, status: "ok",
          } satisfies proto.AgentAssignResult));
        }
      }).catch((e) => {
        logger.error("connector self-assign failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_AGENT_REMOVE: {
      if (!agent.connectorID || agent.pairing) {
        sendError(agent.ws, msg.id, proto.ERR_UNAUTHORIZED, "connector connections only");
        break;
      }
      if (!hub.db) {
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
        break;
      }
      const params = proto.decodeParams<proto.AgentRemoveParams>(msg);
      const db = hub.db;
      void (async () => {
        const row = await db.getAgentRow(params.agent_id);
        if (!row || row.connector_id !== agent.connectorID) {
          sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not hosted by this connector");
          return;
        }
        await doRemoveAgent(hub, db, row);
        sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
      })().catch((e) => {
        logger.error("connector self-remove failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_CAPABILITIES_UPDATED: {
      const params = proto.decodeParams<proto.CapabilitiesUpdatedParams>(msg);
      const a = hub.agents.get(params.agent_id || agent.id);
      // payload 里的 agent_id 只认本连接托管的 agent：连接鉴权 ≠ 内容鉴权，
      // 信任他人 agent_id 会覆盖对方能力声明并刷新对方注册表
      if (a && a.ws !== agent.ws) {
        logger.warn("capabilities update for agent not served by this connection, ignored", {
          from: agent.id, agent_id: a.id,
        });
        break;
      }
      if (params.session_id) {
        // C1 两级作用域：带 session_id 的是该 session/workdir 的命令与技能快照，
        // 不覆盖 Agent 全局能力（admin.agentList 仍反映全局层），仅推给页面做两层合并。
        // 可见性与 agent 事件一致：属主 + 全量 admin。
        if (a) {
          const notif = proto.newNotification(proto.METHOD_CAPABILITIES_UPDATED, {
            agent_id: a.id,
            session_id: params.session_id,
            capabilities: params.capabilities,
          } satisfies proto.CapabilitiesUpdatedParams);
          hub.forwardToUsers(a.ownerID, notif);
          for (const u of hub.users.values()) {
            if (u.isAdmin && !u.ownOnly && u.userID !== a.ownerID) hub.trySend(u.ws, notif);
          }
        }
        break;
      }
      if (a) {
        a.capabilities = params.capabilities;
        // capabilities 变化立即刷注册表：多实例下其他实例不等心跳节流（TTL/3）就能看到新命令
        hub.refreshAgentRegistry(a, true);
      }
      hub.broadcastAgentList();
      break;
    }

    case proto.METHOD_HEARTBEAT: {
      agent.lastHeartbeat = Date.now();
      const params = proto.decodeParams<proto.HeartbeatParams>(msg);
      // 一条连接可能托管多个 agent（connector 模式）：按 agent_id + 同 ws 全部续命
      const targets = new Set<AgentConn>();
      if (params.agent_id) {
        const a = hub.agents.get(params.agent_id);
        if (a && a.ws === agent.ws) targets.add(a);
      }
      for (const a of hub.agents.values()) {
        if (a.ws === agent.ws) targets.add(a);
      }
      for (const a of targets) {
        a.lastHeartbeat = Date.now();
        hub.refreshAgentRegistry(a);
        hub.touchAgentThrottled(a);
      }
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_STATUS: {
      agent.lastHeartbeat = Date.now();
      const params = proto.decodeParams<proto.StatusParams>(msg);
      const a = hub.agents.get(params.agent_id || agent.id);
      // 同上：自报下线/改状态只对本连接托管的 agent 生效，否则可把他人 agent 注销下线
      if (a && a.ws !== agent.ws) {
        logger.warn("status update for agent not served by this connection, ignored", {
          from: agent.id, agent_id: a.id, status: params.status,
        });
        break;
      }
      if (a && params.status === proto.AGENT_STATUS_OFFLINE) {
        // Agent 自报下线（本地服务死亡）：注销而非只改状态。connector 连接还活着时
        // 心跳会给同 ws 的所有 agent 续命，不注销会永远显示在线
        hub.unregisterAgent(a.id);
      } else if (a && params.status && params.status !== a.status) {
        a.status = params.status;
        hub.refreshAgentRegistry(a, true);
        hub.touchAgentThrottled(a, true);
        hub.broadcastAgentList();
      }
      if (msg.id) {
        sendMsg(agent.ws, proto.newResponse(msg.id, { status: "ok" }));
      }
      break;
    }

    case proto.METHOD_AGENT_TASK_INVOKE: {
      const params = proto.decodeParams<proto.AgentTaskInvokeParams>(msg);
      void handleAgentTaskInvoke(hub, agent, msg, params).catch((e) => {
        logger.error("agent.task.invoke failed", { error: String(e) });
        sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_PROGRESS: {
      const params = proto.decodeParams<proto.ProgressParams>(msg);
      const value = params.value;
      if (!value) break;
      if (!value.agent_id) value.agent_id = agent.id;
      // 多 agent 共享连接（connector）：归属/缓冲按进度里的 agent_id 定位。
      // agent_id 必须指向本连接托管的 agent（或本连接自身）：伪造他人 agent_id
      // 会把进度 chunk 写进对方会话缓冲、甚至提前完结对方任务，必须忽略。
      // 自身 id 允许注册表 miss（自报下线后补发 done 的竞态），保持原兜底语义。
      let src: AgentConn;
      if (value.agent_id === agent.id) {
        src = hub.agents.get(value.agent_id) ?? agent;
      } else {
        const hosted = hub.agents.get(value.agent_id);
        if (!hosted || hosted.ws !== agent.ws) {
          logger.warn("progress for agent not served by this connection, ignored", {
            from: agent.id, agent_id: value.agent_id, task_id: value.task_id,
          });
          break;
        }
        src = hosted;
      }
      // 进度还必须来自服务该任务的 agent：拿自己的 agent_id 配他人 task_id
      // 同样会污染对方任务缓冲并把伪造内容转发给任务发起者
      const ts = hub.tasks.get(value.task_id);
      if (ts && ts.agentID !== src.id) {
        logger.warn("progress task not served by this agent, ignored", {
          from: src.id, task_id: value.task_id, task_agent: ts.agentID,
        });
        break;
      }
      const progress: proto.AdminProgressParams = {
        task_id: value.task_id,
        type: value.type,
        agent_id: value.agent_id,
        session_id: value.session_id,
        context_id: value.context_id,
        content: value.content,
        name: value.name,
        arguments: value.arguments,
        confirm_id: value.confirm_id,
        prompt_id: value.prompt_id,
        options: value.options,
        block_id: value.block_id,
        blocks: value.blocks,
        percentage: value.percentage,
        done: value.done,
        error: value.error,
        reason: value.reason,
      };
      const notif = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, progress);
      if (ts?.groupID) progress.group_id = ts.groupID;
      if (ts?.parentTaskID) progress.parent_task_id = ts.parentTaskID;
      hub.forwardToUsers(src.ownerID, notif);
      // 跨属主任务（admin 操作他人 agent）：进度同时发给任务发起者
      if (ts && ts.ownerID !== src.ownerID) hub.forwardToUsers(ts.ownerID, notif);
      {
        // 落库归因优先取网关侧登记的会话：thread 续聊子任务的目标侧会话不落库，
        // 进度归因到群/父会话（群里可见）；同时杜绝 agent 自报 session_id 注入他人会话
        const sessionID = ts?.sessionID ?? value.session_id ?? "";
        // confirm_cancelled 是撤销信号：标记待决 chunk 后不单独落库为 chunk
        if (value.type === proto.CHUNK_TYPE_CONFIRM_CANCELLED) {
          hub.markCancelledChunks(value.task_id, value.confirm_id ?? "",
            value.reason ?? proto.CONFIRM_CANCEL_REASON_TASK_CANCELLED);
        } else if (sessionID !== "") {
          hub.bufferProgressChunk(value.task_id, src.ownerID, src.id, sessionID,
            progress as unknown as proto.LocalAgentChunk);
        }
      }
      if (value.done || (value.error !== undefined && value.error !== "")) {
        if (ts) hub.notifySubtaskResult(value.task_id, ts, value.error);
        hub.finishRun(value.task_id, value.error !== undefined && value.error !== "" ? "failed" : "completed", value.error);
        hub.observeTaskEnd(value.task_id, value.error !== undefined && value.error !== "" ? "failed" : "completed");
        hub.settleWaiters(value.task_id, value.error); // 模板引擎等待者（缓冲 flush 前唤醒）
        hub.untrackTask(value.task_id);
        hub.flushTaskBuffer(value.task_id, value.error);
      }
      break;
    }

    default: {
      if (msg.id) {
        // agent 以错误响应拒绝 agent.chat：先清理关联任务条目（条目随后即被删除）
        if (msg.error) hub.cleanupRejectedTask(msg.id, msg.error.message ?? "");
        hub.forwardToPendingUser(msg.id, msg);
      }
    }
  }
}


export function handleUserMessage(hub: Hub, user: UserConn, raw: string): void {  let msg: proto.Message;
  try {
    msg = JSON.parse(raw) as proto.Message;
  } catch {
    sendError(user.ws, "", proto.ERR_PARSE_ERROR, "parse error");
    return;
  }
  user.lastHeartbeat = Date.now();

  if (msg.jsonrpc !== proto.VERSION) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_REQUEST, "invalid jsonrpc version");
    return;
  }

  switch (msg.method) {
    case proto.METHOD_TASK_CREATE: {
      void handleTaskCreate(hub, user, msg).catch((e) => {
        logger.error("task.create failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_TASK_CANCEL: {
      const cancelParams = proto.decodeParams<proto.TaskCancelParams>(msg);
      if (cancelParams.group_id) {
        handleGroupTaskCancel(hub, user, msg, cancelParams);
        break;
      }
      // C6：单 agent 任务取消同样按 parentTaskID 级联子任务（群路径在 handleGroupTaskCancel 内收敛）。
      // 属主判定与群路径一致：本人或 admin 可级联他人任务的子任务。
      if (cancelParams.task_id) {
        hub.cascadeCancelSubtasks(cancelParams.task_id, (ts) => ts.ownerID === user.userID || user.isAdmin);
      }
      void handleTaskForward(hub, user, msg, proto.METHOD_AGENT_CANCEL, cancelParams).catch((e) => {
        logger.error("task.cancel failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_TASK_RESPOND: {
      const params = proto.decodeParams<proto.TaskRespondParams>(msg);
      hub.markRespondedChunk(params.task_id, params.confirm_id ?? "", params.prompt_id ?? "", params.block_id ?? "", params.response);
      void handleTaskForward(hub, user, msg, proto.METHOD_AGENT_RESPOND, {
        task_id: params.task_id,
        session_id: params.session_id,
        confirm_id: params.confirm_id,
        prompt_id: params.prompt_id,
        block_id: params.block_id,
        action_id: params.action_id,
        response: params.response,
      } satisfies proto.AgentRespondParams, params.agent_id).catch((e) => {
        logger.error("task.respond failed", { error: String(e) });
        sendError(user.ws, msg.id, proto.ERR_INTERNAL_ERROR, "internal error");
      });
      break;
    }

    case proto.METHOD_SESSION_LIST:
      withDb(hub, user, msg, (db) => handleSessionList(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_CREATE:
      withDb(hub, user, msg, (db) => handleSessionCreate(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_RENAME:
      withDb(hub, user, msg, (db) => handleSessionRename(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_SET_WORKDIR:
      withDb(hub, user, msg, (db) => handleSessionSetWorkdir(hub, user, msg, db));
      break;

    case proto.METHOD_SESSION_DELETE:
      withDb(hub, user, msg, (db) => handleSessionDelete(hub, user, msg, db));
      break;

    case proto.METHOD_MESSAGE_LIST:
      withDb(hub, user, msg, (db) => handleMessageList(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_CREATE:
      withDb(hub, user, msg, (db) => handleGroupCreate(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_LIST:
      withDb(hub, user, msg, (db) => handleGroupList(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_DETAIL:
      withDb(hub, user, msg, (db) => handleGroupDetail(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_ADD:
      withDb(hub, user, msg, (db) => handleGroupAdd(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_REMOVE:
      withDb(hub, user, msg, (db) => handleGroupRemove(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_RENAME:
      withDb(hub, user, msg, (db) => handleGroupRename(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_SET_MANAGER:
      withDb(hub, user, msg, (db) => handleGroupSetManager(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_SET_DELEGATES:
      withDb(hub, user, msg, (db) => handleGroupSetDelegates(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_DELETE:
      withDb(hub, user, msg, (db) => handleGroupDelete(hub, user, msg, db));
      break;

    case proto.METHOD_GROUP_RUN:
      withDb(hub, user, msg, (db) => handleGroupRun(hub, user, msg, db));
      break;

    case proto.METHOD_RUN_LIST:
      withDb(hub, user, msg, (db) => handleRunList(hub, user, msg, db));
      break;

    case proto.METHOD_USER_LIST:
      withDb(hub, user, msg, (db) => handleUserList(hub, user, msg, db));
      break;

    case proto.METHOD_USER_CREATE:
      withDb(hub, user, msg, (db) => handleUserCreate(hub, user, msg, db));
      break;

    case proto.METHOD_USER_DISABLE:
      withDb(hub, user, msg, (db) => handleUserDisable(hub, user, msg, db));
      break;

    case proto.METHOD_USER_RESET_PASSWORD:
      withDb(hub, user, msg, (db) => handleUserResetPassword(hub, user, msg, db));
      break;

    case proto.METHOD_USER_CHANGE_PASSWORD:
      withDb(hub, user, msg, (db) => handleUserChangePassword(hub, user, msg, db));
      break;

    case proto.METHOD_USER_SET_ROLE:
      withDb(hub, user, msg, (db) => handleUserSetRole(hub, user, msg, db));
      break;

    case proto.METHOD_USER_DELETE:
      withDb(hub, user, msg, (db) => handleUserDelete(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_LIST:
      withDb(hub, user, msg, (db) => handleAgentList(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_DISCONNECT:
      withDb(hub, user, msg, (db) => handleAgentDisconnect(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REASSIGN:
      withDb(hub, user, msg, (db) => handleAgentReassign(hub, user, msg, db));
      break;

    case proto.METHOD_ADMIN_OVERVIEW:
      withDb(hub, user, msg, (db) => handleAdminOverview(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_CREATE:
      withDb(hub, user, msg, (db) => handleDeviceKeyCreate(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_LIST:
      withDb(hub, user, msg, (db) => handleDeviceKeyList(hub, user, msg, db));
      break;

    case proto.METHOD_DEVICE_KEY_REVOKE:
      withDb(hub, user, msg, (db) => handleDeviceKeyRevoke(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_LIST:
      withDb(hub, user, msg, (db) => handleBrandList(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_CREATE:
      withDb(hub, user, msg, (db) => handleBrandCreate(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_UPDATE:
      withDb(hub, user, msg, (db) => handleBrandUpdate(hub, user, msg, db));
      break;

    case proto.METHOD_BRAND_DELETE:
      withDb(hub, user, msg, (db) => handleBrandDelete(hub, user, msg, db));
      break;

    case proto.METHOD_PRODUCT_PUSH:
      withDb(hub, user, msg, (db) => handleProductPush(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_APPROVE:
      withDb(hub, user, msg, (db) => handleAgentApprove(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REJECT:
      withDb(hub, user, msg, (db) => handleAgentReject(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_LIST:
      withDb(hub, user, msg, (db) => handleConnectorList(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_ASSIGN:
      withDb(hub, user, msg, (db) => handleAgentAssign(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_REMOVE:
      withDb(hub, user, msg, (db) => handleAgentRemove(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_RESTART:
      withDb(hub, user, msg, (db) => handleAgentRestart(hub, user, msg, db));
      break;

    case proto.METHOD_AGENT_SET_NICKNAME:
      withDb(hub, user, msg, (db) => handleAgentSetNickname(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_CREATE:
      withDb(hub, user, msg, (db) => handlePairingCreate(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_LIST:
      withDb(hub, user, msg, (db) => handlePairingList(hub, user, msg, db));
      break;

    case proto.METHOD_PAIRING_DELETE:
      withDb(hub, user, msg, (db) => handlePairingDelete(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_PENDING_LIST:
      withDb(hub, user, msg, (db) => handleConnectorPendingList(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_APPROVE:
      withDb(hub, user, msg, (db) => handleConnectorApprove(hub, user, msg, db));
      break;

    case proto.METHOD_CONNECTOR_REJECT:
      withDb(hub, user, msg, (db) => handleConnectorReject(hub, user, msg, db));
      break;

    default:
      sendError(user.ws, msg.id, proto.ERR_METHOD_NOT_FOUND, `method not found: ${msg.method}`);
  }
}

