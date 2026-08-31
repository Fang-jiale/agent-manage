// Agent/品牌/设备密钥/配对码/connector 相关 RPC 与注册流程。
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import * as proto from "../protocol.ts";
import type { Db, DbAgent, DbAgentBrand, DbPairingCode } from "../db.ts";
import { logger } from "../util.ts";
import type { Hub } from "./hub.ts";
import { sendMsg, sendError } from "./hub.ts";
import type { AgentConn, UserConn, PendingPair } from "./types.ts";
import { MAX_GROUP_FANOUT } from "./types.ts";
import { validProductBrand, validProductVersion, scanProductCatalog } from "./products.ts";
import { requireAdmin } from "./users.ts";

export async function handleAgentList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  // admin 可查全量（可按 owner_id 过滤）；普通用户强制限定为本人名下
  const isAdmin = await hub.isAdminUser(user.userID, db);
  const params = proto.decodeParams<proto.AdminAgentListParams>(msg);
  const limit = Math.min(Math.max(params.limit ?? 50, 1), 200);
  const offset = Math.max(params.offset ?? 0, 0);
  const ownerFilter = isAdmin ? (params.owner_id || undefined) : user.userID;
  const { agents, total } = await db.listAgentsPaged({
    ownerID: ownerFilter,
    status: params.status || undefined,
    query: params.query || undefined,
    limit,
    offset,
  });
  const nicks = await db.listNicknamesForOwner(ownerFilter ?? user.userID);
  // 实时状态优先：内存/注册表覆盖 DB 行的展示状态，DB 提供离线/历史记录
  const liveByID = new Map<string, proto.AgentInfo>();
  for (const a of await hub.agentList()) liveByID.set(a.id, a);
  const rows: proto.AdminAgentInfo[] = agents.map((row) => {
    const live = liveByID.get(row.id);
    let caps: proto.Capability[] = [];
    let plat: proto.PlatformInfo | undefined;
    try { caps = row.capabilities ? JSON.parse(row.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
    try { plat = row.platform ? JSON.parse(row.platform) as proto.PlatformInfo : undefined; } catch { /* 忽略坏数据 */ }
    const brand = row.brand_id ? hub.brands.get(row.brand_id) : undefined;
    return {
      id: row.id,
      owner_id: live?.owner_id ?? row.owner_id,
      name: live?.name ?? row.name,
      nickname: nicks.get(row.id) ?? null,
      status: live?.status ?? (row.approval_status === "pending" ? "pending" : row.status),
      capabilities: live?.capabilities ?? caps,
      platform: live?.platform ?? plat,
      last_heartbeat: live?.last_heartbeat ?? new Date(row.last_seen).toISOString(),
      first_seen: row.first_seen,
      last_seen: row.last_seen,
      online: live !== undefined,
      last_ip: row.last_ip ?? null,
      brand_id: row.brand_id,
      brand_name: brand?.name ?? null,
      logo_url: brand?.logo_url ?? null,
      approval_status: row.approval_status,
      connector_id: row.connector_id,
    };
  });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { agents: rows, total } satisfies proto.AdminAgentListResult));
}

export async function handleAgentDisconnect(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentDisconnectParams>(msg);
  const local = hub.getAgent(params.agent_id);
  if (!local) {
    // 一期限制：只能断连落在本实例上的 agent（跨实例需要 bus 指令通道）
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "agent not connected to this instance");
    return;
  }
  if (local.connectorID) {
    // connector 托管的 agent 与兄弟 agent 共享连接，断连会误伤；移除请用 agent.remove
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "connector-managed agent: use agent.remove");
    return;
  }
  local.ws.close(4001, "disconnected by admin");
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleAgentReassign(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentReassignParams>(msg);
  if (!params.agent_id || !params.owner_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent_id and owner_id required");
    return;
  }
  if (!(await db.getUserById(params.owner_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "new owner not found");
    return;
  }
  if (!(await db.reassignAgent(params.agent_id, params.owner_id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  // 在线连接同步改归属。注意：JWT 重连会按 token 夺回归属（upsertAgent 以凭证为准），
  // 所以 reassign 只对设备密钥接入的 agent 是持久的。
  const local = hub.getAgent(params.agent_id);
  if (local) {
    local.ownerID = params.owner_id;
    hub.refreshAgentRegistry(local, true);
  }
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleAdminOverview(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const [users, agentsTotal, liveAgents] = await Promise.all([
    db.listUsers(),
    db.countAgents(),
    hub.agentList(),
  ]);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    users_total: users.length,
    agents_total: agentsTotal,
    agents_online: liveAgents.length,
    users_connected: hub.users.size,
    tasks_active: hub.tasks.size,
  } satisfies proto.OverviewResult));
}

// ---- 设备密钥 ----

// 明文形如 amk_<base64url(24B)>，只在创建时返回一次；库中只存 sha256
export function generateDeviceKey(): { plaintext: string; hash: string } {
  const plaintext = "amk_" + crypto.randomBytes(24).toString("base64url");
  return { plaintext, hash: hashDeviceKey(plaintext) };
}

export function hashDeviceKey(plaintext: string): string {
  return crypto.createHash("sha256").update(plaintext).digest("hex");
}

export function deviceKeyInfoOf(k: { id: string; owner_id: string; name: string; created_at: number; last_used_at: number | null; disabled: number }): proto.DeviceKeyInfo {
  return {
    id: k.id,
    owner_id: k.owner_id,
    name: k.name,
    created_at: k.created_at,
    last_used_at: k.last_used_at,
    disabled: k.disabled === 1,
  };
}

export async function handleDeviceKeyCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyCreateParams>(msg);
  if (!params.name || !params.name.trim()) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.getUserById(params.owner_id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "owner not found");
      return;
    }
    ownerID = params.owner_id;
  }
  if (!hub.deviceKeyLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many keys created, please slow down");
    return;
  }
  const { plaintext, hash } = generateDeviceKey();
  const id = crypto.randomUUID();
  await db.createDeviceKey({ id, owner_id: ownerID, name: params.name.trim(), key_hash: hash });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { id, key: plaintext } satisfies proto.DeviceKeyCreateResult));
}

export async function handleDeviceKeyList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyListParams>(msg);
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    ownerID = params.owner_id;
  }
  const keys = await db.listDeviceKeys(ownerID);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    keys: keys.map(deviceKeyInfoOf),
  } satisfies proto.DeviceKeyListResult));
}

export async function handleDeviceKeyRevoke(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.DeviceKeyRevokeParams>(msg);
  const keys = await db.listDeviceKeys(user.userID);
  let target = keys.find((k) => k.id === params.id);
  if (!target) {
    // 非本人需 admin 才能吊销（全表按 id 直接置禁用）
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.setDeviceKeyDisabled(params.id, true))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "key not found");
      return;
    }
  } else {
    await db.setDeviceKeyDisabled(target.id, true);
  }
  // 踢掉使用该密钥的在线 agent 连接（含其他实例）
  hub.kickDeviceKey(params.id);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 配对接入：配对码 CRUD + 待接入审批 ----

export function pairingCodeInfoOf(c: DbPairingCode): proto.PairingCodeInfo {
  return {
    id: c.id,
    owner_id: c.owner_id,
    expires_at: c.expires_at,
    used_at: c.used_at,
    created_at: c.created_at,
  };
}

export async function handlePairingCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.PairingCodeCreateParams>(msg);
  let ownerID = user.userID;
  if (params.owner_id && params.owner_id !== user.userID) {
    if (!(await requireAdmin(hub, user, msg, db))) return;
    if (!(await db.getUserById(params.owner_id))) {
      sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "owner not found");
      return;
    }
    ownerID = params.owner_id;
  }
  if (!hub.deviceKeyLimiter.allow(user.userID)) {
    sendError(user.ws, msg.id, proto.ERR_RATE_LIMITED, "too many codes created, please slow down");
    return;
  }
  const ttlMs = Math.min(Math.max(params.ttl_seconds ?? 86_400, 60), 7 * 86_400) * 1000;
  const { plaintext, hash } = generateDeviceKey();
  const id = crypto.randomUUID();
  const expiresAt = Date.now() + ttlMs;
  await db.createPairingCode({ id, owner_id: ownerID, code_hash: hash, expires_at: expiresAt });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    id, code: plaintext, owner_id: ownerID, expires_at: expiresAt,
  } satisfies proto.PairingCodeCreateResult));
}

export async function handlePairingList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const codes = await db.listPairingCodes(user.userID);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    codes: codes.map(pairingCodeInfoOf),
  } satisfies proto.PairingCodeListResult));
}

export async function handlePairingDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.PairingCodeDeleteParams>(msg);
  const codes = await db.listPairingCodes(user.userID);
  if (!codes.some((c) => c.id === params.id)) {
    // 非本人需 admin 才能作废
    if (!(await requireAdmin(hub, user, msg, db))) return;
  }
  if (!(await db.deletePairingCode(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "code not found");
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export function pendingConnectorInfoOf(p: PendingPair): proto.PendingConnectorInfo {
  return {
    connector_id: p.connectorID,
    owner_id: p.ownerID,
    code_id: p.codeID,
    platform: p.platform,
    version: p.version,
    ip: p.ip,
    paired_at: p.pairedAt,
  };
}

export async function handleConnectorPendingList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    connectors: [...hub.pendingPairs.values()].map(pendingConnectorInfoOf),
  } satisfies proto.ConnectorPendingListResult));
}

export async function handleConnectorApprove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.ConnectorApproveParams>(msg);
  const p = hub.pendingPairs.get(params.connector_id);
  if (!p) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "pending connector not found");
    return;
  }
  // 签发设备密钥：owner = 配对码归属用户，明文只随 connector.credential 推一次
  const { plaintext, hash } = generateDeviceKey();
  await db.createDeviceKey({
    id: crypto.randomUUID(), owner_id: p.ownerID,
    name: `connector:${p.connectorID}`, key_hash: hash,
  });
  await db.markPairingCodeUsed(p.codeID);
  hub.pendingPairs.delete(p.connectorID);
  sendMsg(p.conn.ws, proto.newNotification(proto.METHOD_CONNECTOR_CREDENTIAL, {
    connector_id: p.connectorID, key: plaintext,
  } satisfies proto.ConnectorCredentialParams));
  logger.info("connector approved", { connector_id: p.connectorID, owner_id: p.ownerID, by: user.userID });
  // 凭证已投递，配对连接使命完成；client 落盘后用 key 重连走 connector.hello
  setTimeout(() => p.conn.ws.close(1000, "credential delivered"), 500).unref();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleConnectorReject(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.ConnectorApproveParams>(msg);
  const p = hub.pendingPairs.get(params.connector_id);
  if (!p) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "pending connector not found");
    return;
  }
  hub.pendingPairs.delete(p.connectorID);
  // 配对码不消耗，持码方可换 connector_id 重试或让管理员重新审批
  logger.info("connector rejected", { connector_id: p.connectorID, by: user.userID });
  p.conn.ws.close(4001, "pairing rejected");
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// client 凭配对码接入（?pair=1 无凭证连接）：校验码 → 挂起等审批
export async function handleConnectorPair(hub: Hub, agent: AgentConn, msg: proto.Message): Promise<void> {
  if (!hub.db) {
    sendError(agent.ws, msg.id, proto.ERR_INTERNAL_ERROR, "storage unavailable");
    return;
  }
  const params = proto.decodeParams<proto.ConnectorPairParams>(msg);
  const connectorID = params.connector_id?.trim();
  if (!connectorID || !params.code?.trim()) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "code and connector_id required");
    return;
  }
  const code = await hub.db.getPairingCodeByHash(hashDeviceKey(params.code.trim()));
  if (!code || code.used_at !== null || code.expires_at <= Date.now()) {
    sendError(agent.ws, msg.id, proto.ERR_INVALID_PARAMS, "invalid or expired pairing code");
    agent.ws.close(4001, "invalid pairing code");
    return;
  }
  // 同 connector_id 重复 pair：踢掉旧的挂起连接
  const old = hub.pendingPairs.get(connectorID);
  if (old && old.conn.ws !== agent.ws) old.conn.ws.close(4001, "superseded by new pairing");
  hub.pendingPairs.set(connectorID, {
    connectorID,
    ownerID: code.owner_id,
    codeID: code.id,
    conn: agent,
    platform: params.platform,
    version: params.version,
    ip: agent.ip,
    pairedAt: Date.now(),
  });
  logger.info("connector pairing pending", { connector_id: connectorID, owner_id: code.owner_id });
  sendMsg(agent.ws, proto.newResponse(msg.id ?? "", { status: "pending" } satisfies proto.ConnectorPairResult));
}


export function brandInfoOf(b: DbAgentBrand): proto.BrandInfo {
  let caps: proto.Capability[] = [];
  try { caps = b.capabilities ? JSON.parse(b.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
  return {
    id: b.id,
    name: b.name,
    description: b.description,
    logo_url: b.logo_url,
    capabilities: caps,
    conn_type: b.conn_type || "stdio",
    launch_cmd: b.launch_cmd ?? null,
    endpoint: b.endpoint ?? null,
    disabled: b.disabled === 1,
    created_at: b.created_at,
    updated_at: b.updated_at,
  };
}

export async function handleBrandList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  // 只读目录：任何登录用户可读（发起配对/了解可接入的品牌）；写操作仍仅 admin
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    brands: (await db.listBrands()).map(brandInfoOf),
  } satisfies proto.BrandListResult));
}

export async function handleBrandCreate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandCreateParams>(msg);
  const name = params.name?.trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  if (await db.getBrandByName(name)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand name already exists");
    return;
  }
  const connType = params.conn_type ?? "stdio";
  if (!["stdio", "http", "ws", "web", "app"].includes(connType)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "conn_type must be stdio|http|ws|web|app");
    return;
  }
  const b: DbAgentBrand = {
    id: crypto.randomUUID(),
    name,
    description: params.description ?? "",
    logo_url: params.logo_url ?? null,
    capabilities: JSON.stringify(params.capabilities ?? []),
    conn_type: connType,
    launch_cmd: params.launch_cmd ?? null,
    endpoint: params.endpoint ?? null,
    disabled: 0,
    created_at: Date.now(),
    updated_at: Date.now(),
  };
  await db.createBrand(b);
  await hub.reloadBrands();
  hub.broadcastAgentList(); // 首个品牌会开启治理模式，列表刷新带品牌信息
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", brandInfoOf(b)));
}

export async function handleBrandUpdate(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandUpdateParams>(msg);
  const existing = await db.getBrandById(params.id);
  if (!existing) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand not found");
    return;
  }
  const name = params.name?.trim();
  if (!name) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "name required");
    return;
  }
  const conflict = await db.getBrandByName(name);
  if (conflict && conflict.id !== params.id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand name already exists");
    return;
  }
  const connType = params.conn_type ?? "stdio";
  if (!["stdio", "http", "ws", "web", "app"].includes(connType)) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "conn_type must be stdio|http|ws|web|app");
    return;
  }
  await db.updateBrand(params.id, {
    name,
    description: params.description ?? "",
    logo_url: params.logo_url ?? null,
    capabilities: JSON.stringify(params.capabilities ?? []),
    conn_type: connType,
    launch_cmd: params.launch_cmd ?? null,
    endpoint: params.endpoint ?? null,
    disabled: params.disabled ?? false,
  });
  await hub.reloadBrands();
  hub.broadcastAgentList();
  // launch_cmd/capabilities 可能变了：全量重推各 connector 的目标集，client 侧对账重建
  for (const connectorID of hub.connectors.keys()) {
    await hub.pushConnectorSync(connectorID);
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 远程推送产品更新：广播给所有在线 connector，各端自行决定纳管升级/原地更新/忽略
export async function handleProductPush(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  // 升级指令会洒向全部在线 connector、触发终端侧下载替换，必须与其他管理动作
  // 一致走 admin 校验（此前是唯一漏掉 requireAdmin 的变更类方法）。
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<{ brand?: string; version?: string }>(msg);
  const brand = String(params.brand ?? "");
  const version = String(params.version ?? "");
  if (!brand || !version) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand/version required");
    return;
  }
  // 只允许推送目录中真实存在的包：任意 brand/version 字符串会让所有终端
  // 白跑一轮下载/替换。先过格式白名单再拼路径，防目录穿越探测。
  if (hub.productsDir && validProductBrand(brand) && validProductVersion(version)
    && !fs.existsSync(path.join(hub.productsDir, brand, version, "manifest.json"))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand/version not in catalog");
    return;
  }
  const note = proto.newNotification(proto.METHOD_PRODUCT_PUSH, { brand, version });
  let n = 0;
  for (const c of hub.connectors.values()) {
    hub.trySend(c.ws, note);
    n++;
  }
  logger.info("product update pushed", { by: user.userID, brand, version, connectors: n });
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", pushed: n }));
}

export async function handleBrandDelete(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.BrandDeleteParams>(msg);
  if (!(await db.deleteBrand(params.id))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "brand not found");
    return;
  }
  await hub.reloadBrands();
  hub.broadcastAgentList();
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- 注册审批 ----

export async function handleAgentApprove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentApprovalParams>(msg);
  if (!(await db.setAgentApproval(params.agent_id, "approved"))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  hub.applyAgentApproval(params.agent_id, "approved");
  if (hub.bus) {
    hub.bus.publishAgentApproval(params.agent_id, "approved")
      .catch((e) => logger.error("bus publish approval failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

export async function handleAgentReject(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const params = proto.decodeParams<proto.AgentApprovalParams>(msg);
  if (!(await db.setAgentApproval(params.agent_id, "rejected"))) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  hub.applyAgentApproval(params.agent_id, "rejected");
  if (hub.bus) {
    hub.bus.publishAgentApproval(params.agent_id, "rejected")
      .catch((e) => logger.error("bus publish approval failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// ---- connector 与实例分配 ----

export async function handleConnectorList(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  if (!(await requireAdmin(hub, user, msg, db))) return;
  const connectors: proto.ConnectorInfo[] = [...hub.connectors.values()].map((c) => ({
    id: c.connectorID ?? "",
    owner_id: c.ownerID,
    platform: c.platform,
    ip: c.ip,
    agents: [...hub.agents.values()].filter((a) => a.connectorID === c.connectorID).length,
    last_heartbeat: new Date(c.lastHeartbeat).toISOString(),
  }));
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { connectors } satisfies proto.ConnectorListResult));
}

// 分配主体：admin 通道与 connector 自助通道共用（鉴权由调用方负责）
export async function doAssignAgent(hub: Hub, db: Db, connector: AgentConn, params: proto.AgentAssignParams)
  : Promise<{ agent_id: string } | { error: string }> {
  const brand = hub.brands.get(params.brand_id);
  if (!brand || brand.disabled === 1) return { error: "unknown or disabled brand" };
  let agentID = params.name?.trim().replace(/[^\w-]/g, "-") ?? "";
  if (agentID !== "") {
    if (await db.getAgentRow(agentID)) return { error: "agent id already taken" };
  } else {
    // 默认 <品牌名>-<短随机>，撞库则重试
    for (;;) {
      agentID = `${brand.name.replace(/[^\w-]/g, "-")}-${crypto.randomUUID().slice(0, 6)}`;
      if (!(await db.getAgentRow(agentID))) break;
    }
  }
  await db.assignAgent({
    id: agentID,
    owner_id: connector.ownerID,
    name: agentID,
    brand_id: brand.id,
    connector_id: connector.connectorID ?? "",
  });
  await hub.pushConnectorSync(connector.connectorID ?? "");
  return { agent_id: agentID };
}

export async function handleAgentAssign(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentAssignParams>(msg);
  const connector = params.connector_id ? hub.connectors.get(params.connector_id) : undefined;
  if (!connector) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "connector not online");
    return;
  }
  // connector 属主可自助分配，跨属主需 admin
  if (connector.ownerID !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  const r = await doAssignAgent(hub, db, connector, params);
  if ("error" in r) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, r.error);
    return;
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", {
    agent_id: r.agent_id,
    status: "ok",
  } satisfies proto.AgentAssignResult));
}

// 移除主体：注意不能关 ws——connector 托管的 agent 与 connector 及其他 agent 共享连接。
// 网关侧直接注销，client 随后由 sync 对账下线（kill shim）。
export async function doRemoveAgent(hub: Hub, db: Db, row: DbAgent): Promise<void> {
  await db.unassignAgent(row.id);
  hub.unregisterAgent(row.id);
  await hub.pushConnectorSync(row.connector_id ?? "");
}

export async function handleAgentRemove(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentRemoveParams>(msg);
  const row = await db.getAgentRow(params.agent_id);
  if (!row) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  if (!row.connector_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent is not connector-managed");
    return;
  }
  if (row.owner_id !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  await doRemoveAgent(hub, db, row);
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
}

// 重启 connector 托管实例：不动 DB 行，通知 client 杀掉本地子进程并按原配置重建，
// agent_id / 会话历史 / 审批状态全部保留（适合"程序更新了、命令没变"的场景）
export async function handleAgentRestart(hub: Hub, user: UserConn, msg: proto.Message, db: Db): Promise<void> {
  const params = proto.decodeParams<proto.AgentRestartParams>(msg);
  const row = await db.getAgentRow(params.agent_id);
  if (!row) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent not found");
    return;
  }
  if (!row.connector_id) {
    sendError(user.ws, msg.id, proto.ERR_INVALID_PARAMS, "agent is not connector-managed");
    return;
  }
  if (row.owner_id !== user.userID && !(await requireAdmin(hub, user, msg, db))) return;
  if (!hub.connectors.get(row.connector_id) && !hub.bus) {
    sendError(user.ws, msg.id, proto.ERR_AGENT_NOT_FOUND, "connector not online");
    return;
  }
  const notif = proto.newNotification(proto.METHOD_CONNECTOR_RESTART, {
    agent_id: row.id,
  } satisfies proto.ConnectorRestartParams);
  hub.deliverToLocalConnector(row.connector_id, notif);
  if (hub.bus) {
    hub.bus.publishConnectorSync(row.connector_id, notif)
      .catch((e) => logger.error("bus publish connector restart failed", { error: String(e) }));
  }
  sendMsg(user.ws, proto.newResponse(msg.id ?? "", { status: "ok", agent_id: row.id }));
}

// Agent 注册：治理模式（品牌目录非空）下必须带合法 brand_id，名称/能力以品牌行覆盖；
// client 主动注册进入 pending 待审批，页面分配的实例（agents 行已存在且 approved）直接通过。
// 一条连接可托管多个 agent（connector 模式）：每次注册创建独立 AgentConn，共享 ws。
export async function handleAgentRegister(hub: Hub, base: AgentConn, params: proto.RegisterParams, msg: proto.Message): Promise<void> {
  const reject = (code: number, m: string): void => {
    sendError(base.ws, msg.id, code, m);
    base.ws.close(4001, m);
  };
  if (!params.agent_id) {
    reject(proto.ERR_INVALID_PARAMS, "agent_id required");
    return;
  }
  let brand: DbAgentBrand | undefined;
  if (hub.governanceOn()) {
    if (!hub.db || !params.brand_id) {
      reject(proto.ERR_INVALID_PARAMS, "brand_id required (governance mode)");
      return;
    }
    brand = await hub.db.getBrandById(params.brand_id);
    if (!brand || brand.disabled === 1) {
      reject(proto.ERR_INVALID_PARAMS, "unknown or disabled brand");
      return;
    }
  }
  let approval = "approved";
  const row = hub.db ? await hub.db.getAgentRow(params.agent_id) : undefined;
  // 已存在的 agent 归属他人时拒绝注册：upsert 的 ON DUPLICATE KEY 会用当前凭据
  // 覆写 owner_id，不拦的话任何持有效 token 的用户都能抢注他人 agent_id、
  // 踢掉正主连接并接管路由。admin 转移归属后，旧属主重连同样走此拒绝。
  if (row && row.owner_id !== base.ownerID) {
    logger.warn("agent register rejected: owned by another user", {
      agent_id: params.agent_id, owner: row.owner_id, caller: base.ownerID,
    });
    reject(proto.ERR_UNAUTHORIZED, "agent_id owned by another user");
    return;
  }
  if (hub.governanceOn() && hub.db) {
    if (row?.approval_status === "rejected") {
      reject(proto.ERR_UNAUTHORIZED, "registration rejected");
      return;
    }
    if (!row) {
      await hub.db.createPendingAgent({
        id: params.agent_id,
        owner_id: base.ownerID,
        name: brand?.name ?? params.name ?? params.agent_id,
        brand_id: brand?.id ?? null,
      });
      approval = "pending";
      logger.info("agent pending approval", { agent_id: params.agent_id, owner_id: base.ownerID });
    } else if (row.approval_status === "pending") {
      approval = "pending";
    }
  }
  let brandCaps: proto.Capability[] = [];
  try { brandCaps = brand?.capabilities ? JSON.parse(brand.capabilities) as proto.Capability[] : []; } catch { /* 忽略坏数据 */ }
  const a: AgentConn = {
    id: params.agent_id,
    ownerID: base.ownerID, // Owner comes from token; register params owner is ignored for security.
    name: row?.name || brand?.name
      || (params.name !== "" && params.name !== undefined ? params.name : params.agent_id),
    ws: base.ws,
    capabilities: brand ? brandCaps : (params.capabilities ?? []),
    platform: params.platform ?? base.platform,
    status: proto.AGENT_STATUS_ONLINE,
    lastHeartbeat: Date.now(),
    alive: true,
    deviceKeyID: base.deviceKeyID,
    ip: base.ip,
    brandID: brand?.id ?? row?.brand_id ?? undefined,
    connectorID: row?.connector_id ?? undefined,
    approval,
  };
  // 同 id 已在其他连接上注册（僵尸实例与重启后的新实例并存）：踢掉旧连接。
  // 否则后注册者覆盖路由表，旧实例还以为自己在线，对话被路由到僵尸上。
  // 旧 client 收到 4002 必须退出而非重连，否则两个实例互踢。
  const existing = hub.agents.get(params.agent_id);
  if (existing && existing.ws !== base.ws) {
    logger.warn("agent re-registered on new connection, kicking old", { agent_id: params.agent_id });
    existing.ws.close(4002, "replaced by new connection");
  }
  hub.registerAgent(a);
  sendMsg(base.ws, proto.newResponse(msg.id ?? "", {
    status: "ok",
    server_time: proto.rfc3339Now(),
  } satisfies proto.RegisterResult));
}

