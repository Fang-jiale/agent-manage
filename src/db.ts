// MySQL 存储层：users / sessions / messages 三张表。
// 连接串：mysql://user:pass@host:3306/dbname（AGENT_MANAGE_DATABASE_URL）。

import mysql from "mysql2/promise";

export interface DbUser {
  id: string;
  name: string;
  password_hash: string;
  role: string; // "admin" | "user"
  disabled: number; // 0/1
  created_at: number;
  last_login_at: number | null;
  employee_id: string | null; // 预留：统一认证工号（9 位数字）
  display_name: string | null;
}

export interface DbAgent {
  id: string;
  owner_id: string;
  name: string;
  platform: string | null; // JSON
  capabilities: string | null; // JSON
  status: string; // online | busy | offline
  first_seen: number;
  last_seen: number;
  last_ip: string | null;
  brand_id: string | null;
  connector_id: string | null;
  approval_status: string; // approved | pending | rejected
  is_manager?: number; // 0/1，群组管理者标记（展示用）
}

export interface DbAgentBrand {
  id: string;
  name: string;
  description: string;
  logo_url: string | null;
  capabilities: string | null; // JSON
  conn_type: string; // stdio | http | ws：托管实例与本地服务的连接方式
  launch_cmd: string | null; // conn_type=stdio：本地服务启动命令
  endpoint: string | null; // conn_type=http/ws：本地服务地址
  disabled: number; // 0/1
  created_at: number;
  updated_at: number;
}

export interface DbPairingCode {
  id: string;
  owner_id: string;
  code_hash: string;
  expires_at: number;
  used_at: number | null;
  created_at: number;
}

export interface DbDeviceKey {
  id: string;
  owner_id: string;
  name: string;
  key_hash: string;
  created_at: number;
  last_used_at: number | null;
  disabled: number; // 0/1
}

export interface DbSession {
  id: string;
  owner_id: string;
  agent_id: string; // 单 agent 会话 = agent id；群会话 = "group:<gid>"
  title: string;
  workdir: string | null; // 会话绑定的工作目录（空 = 未绑定，随实例启动目录）
  created_at: number;
  updated_at: number;
  message_count?: number;
}

export interface DbGroup {
  id: string;
  owner_id: string;
  name: string;
  manager_agent_id: string | null;
  created_at: number;
}

export interface DbGroupMember {
  group_id: string;
  agent_id: string;
  added_at: number;
}

export interface DbMessage {
  id: string;
  session_id: string;
  owner_id: string;
  agent_id: string;
  role: string;
  content: string; // JSON 序列化后的消息体
  task_id: string | null;
  created_at: number;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(128) NOT NULL UNIQUE,
  password_hash VARCHAR(256) NOT NULL,
  role VARCHAR(16) NOT NULL DEFAULT 'user',
  disabled TINYINT(1) NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sessions (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  title VARCHAR(256) NOT NULL,
  workdir VARCHAR(512) NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  KEY idx_owner_agent (owner_id, agent_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS messages (
  id VARCHAR(64) PRIMARY KEY,
  session_id VARCHAR(64) NOT NULL,
  owner_id VARCHAR(64) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  role VARCHAR(16) NOT NULL,
  content MEDIUMTEXT NOT NULL,
  task_id VARCHAR(64) NULL,
  created_at BIGINT NOT NULL,
  KEY idx_session (session_id, created_at),
  KEY idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agents (
  id VARCHAR(128) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  name VARCHAR(256) NOT NULL,
  platform TEXT NULL,
  capabilities MEDIUMTEXT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'offline',
  first_seen BIGINT NOT NULL,
  last_seen BIGINT NOT NULL,
  last_ip VARCHAR(64) NULL,
  KEY idx_owner (owner_id, last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS device_keys (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL,
  key_hash VARCHAR(128) NOT NULL UNIQUE,
  created_at BIGINT NOT NULL,
  last_used_at BIGINT NULL,
  disabled TINYINT(1) NOT NULL DEFAULT 0,
  KEY idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_brands (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(128) NOT NULL UNIQUE,
  description VARCHAR(512) NOT NULL DEFAULT '',
  logo_url VARCHAR(512) NULL,
  capabilities MEDIUMTEXT NULL,
  launch_cmd VARCHAR(512) NULL,
  conn_type VARCHAR(16) NOT NULL DEFAULT 'stdio',
  endpoint VARCHAR(512) NULL,
  disabled TINYINT(1) NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pairing_codes (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  code_hash VARCHAR(128) NOT NULL UNIQUE,
  expires_at BIGINT NOT NULL,
  used_at BIGINT NULL,
  created_at BIGINT NOT NULL,
  KEY idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_groups (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL,
  manager_agent_id VARCHAR(128) NULL,
  created_at BIGINT NOT NULL,
  KEY idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_group_members (
  group_id VARCHAR(64) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  added_at BIGINT NOT NULL,
  PRIMARY KEY (group_id, agent_id),
  KEY idx_agent (agent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_nicknames (
  owner_id VARCHAR(64) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  nickname VARCHAR(256) NOT NULL,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (owner_id, agent_id),
  KEY idx_agent (agent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`;

export class Db {
  private pool: mysql.Pool;

  constructor(url: string) {
    this.pool = mysql.createPool({ uri: url, connectionLimit: 10, timezone: "Z" });
  }

  async init(): Promise<void> {
    for (const stmt of SCHEMA.split(";").map((s) => s.trim()).filter((s) => s !== "")) {
      await this.pool.query(stmt);
    }
    // 存量库迁移：补 role/disabled 列，原有 admin 账号提升为管理员
    for (const col of [
      "role VARCHAR(16) NOT NULL DEFAULT 'user'",
      "disabled TINYINT(1) NOT NULL DEFAULT 0",
      "last_login_at BIGINT NULL",
      "employee_id VARCHAR(16) NULL UNIQUE", // 内联 UNIQUE：重复迁移同样报 1060
      "display_name VARCHAR(128) NULL",
    ]) {
      try {
        await this.pool.query(`ALTER TABLE users ADD COLUMN ${col}`);
      } catch (e) {
        if ((e as { errno?: number }).errno !== 1060) throw e; // 1060 = duplicate column
      }
    }
    await this.pool.query("UPDATE users SET role = 'admin' WHERE name = 'admin' AND role <> 'admin'");
    // 存量 agents 表迁移：补 last_ip 列
    try {
      await this.pool.query("ALTER TABLE agents ADD COLUMN last_ip VARCHAR(64) NULL");
    } catch (e) {
      if ((e as { errno?: number }).errno !== 1060) throw e;
    }
    // 品牌治理迁移：品牌/连接器归属 + 审批状态（存量 agent 默认已批准）
    for (const col of [
      "brand_id VARCHAR(64) NULL",
      "connector_id VARCHAR(128) NULL",
      "approval_status VARCHAR(16) NOT NULL DEFAULT 'approved'",
    ]) {
      try {
        await this.pool.query(`ALTER TABLE agents ADD COLUMN ${col}`);
      } catch (e) {
        if ((e as { errno?: number }).errno !== 1060) throw e; // 1060 = duplicate column
      }
    }
    // 品牌托管实例启动命令（存量 agent_brands 表补列）
    for (const col of [
      "launch_cmd VARCHAR(512) NULL",
      "conn_type VARCHAR(16) NOT NULL DEFAULT 'stdio'",
      "endpoint VARCHAR(512) NULL",
    ]) {
      try {
        await this.pool.query(`ALTER TABLE agent_brands ADD COLUMN ${col}`);
      } catch (e) {
        if ((e as { errno?: number }).errno !== 1060) throw e;
      }
    }
    // 群组编排：管理者标记（展示用辅助字段，实际权限以 agent_groups.manager_agent_id 为准）
    try {
      await this.pool.query("ALTER TABLE agents ADD COLUMN is_manager TINYINT(1) NOT NULL DEFAULT 0");
    } catch (e) {
      if ((e as { errno?: number }).errno !== 1060) throw e;
    }
    // 会话绑定工作目录
    try {
      await this.pool.query("ALTER TABLE sessions ADD COLUMN workdir VARCHAR(512) NULL");
    } catch (e) {
      if ((e as { errno?: number }).errno !== 1060) throw e;
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async ping(): Promise<boolean> {
    try {
      await this.pool.query("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }

  async getUserByName(name: string): Promise<DbUser | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM users WHERE name = ?", [name]);
    return (rows as DbUser[])[0];
  }

  async getUserById(id: string): Promise<DbUser | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM users WHERE id = ?", [id]);
    return (rows as DbUser[])[0];
  }

  async createUser(user: { id: string; name: string; password_hash: string; role?: string; employee_id?: string | null; display_name?: string | null }): Promise<void> {
    await this.pool.query(
      "INSERT INTO users (id, name, password_hash, role, created_at, employee_id, display_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [user.id, user.name, user.password_hash, user.role ?? "user", Date.now(), user.employee_id ?? null, user.display_name ?? null],
    );
  }

  async getUserByEmployeeID(employeeID: string): Promise<DbUser | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM users WHERE employee_id = ?", [employeeID]);
    return (rows as DbUser[])[0];
  }

  async listUsers(): Promise<DbUser[]> {
    const [rows] = await this.pool.query("SELECT * FROM users ORDER BY created_at ASC");
    return rows as DbUser[];
  }

  async setUserDisabled(id: string, disabled: boolean): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE users SET disabled = ? WHERE id = ?", [disabled ? 1 : 0, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async setUserPassword(id: string, passwordHash: string): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE users SET password_hash = ? WHERE id = ?", [passwordHash, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async deleteUser(id: string): Promise<void> {
    await this.pool.query("DELETE FROM users WHERE id = ?", [id]);
  }

  // 删除用户时的连带清理：会话/消息/设备密钥/配对码/群组/备注名。
  // agents 行由调用方先行逐个移除（需经 hub 注销在线连接），群成员同时清掉指向这些 agent 的行。
  // 不用跨表 JOIN/子查询比较：历史库表 collation 不一致（unicode_ci vs 0900_ai_ci）会报
  // Illegal mix of collations；先 SELECT id 再 IN 常量列表可完全避开。
  async purgeUserOwnedData(id: string): Promise<void> {
    const [sessRows] = await this.pool.query("SELECT id FROM sessions WHERE owner_id = ?", [id]);
    const sessionIds = (sessRows as { id: string }[]).map((r) => r.id);
    if (sessionIds.length > 0) {
      await this.pool.query("DELETE FROM messages WHERE session_id IN (?)", [sessionIds]);
    }
    await this.pool.query("DELETE FROM sessions WHERE owner_id = ?", [id]);
    const [grpRows] = await this.pool.query("SELECT id FROM agent_groups WHERE owner_id = ?", [id]);
    const groupIds = (grpRows as { id: string }[]).map((r) => r.id);
    if (groupIds.length > 0) {
      await this.pool.query("DELETE FROM agent_group_members WHERE group_id IN (?)", [groupIds]);
    }
    const [agentRows] = await this.pool.query("SELECT id FROM agents WHERE owner_id = ?", [id]);
    const agentIds = (agentRows as { id: string }[]).map((r) => r.id);
    if (agentIds.length > 0) {
      await this.pool.query("DELETE FROM agent_group_members WHERE agent_id IN (?)", [agentIds]);
    }
    await this.pool.query("DELETE FROM agent_groups WHERE owner_id = ?", [id]);
    await this.pool.query("DELETE FROM device_keys WHERE owner_id = ?", [id]);
    await this.pool.query("DELETE FROM pairing_codes WHERE owner_id = ?", [id]);
    await this.pool.query("DELETE FROM agent_nicknames WHERE owner_id = ?", [id]);
  }

  async touchLastLogin(id: string): Promise<void> {
    await this.pool.query("UPDATE users SET last_login_at = ? WHERE id = ?", [Date.now(), id]);
  }

  async setUserRole(id: string, role: string): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE users SET role = ? WHERE id = ?", [role, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async listUsersPaged(opts: { query?: string; limit: number; offset: number }): Promise<{ users: DbUser[]; total: number }> {
    const where = opts.query ? " WHERE name LIKE ? OR display_name LIKE ?" : "";
    const params: unknown[] = [];
    if (opts.query) params.push(`%${opts.query}%`, `%${opts.query}%`);
    const [rows] = await this.pool.query(
      `SELECT * FROM users${where} ORDER BY created_at ASC LIMIT ? OFFSET ?`,
      [...params, opts.limit, opts.offset],
    );
    const [cnt] = await this.pool.query(`SELECT COUNT(*) AS n FROM users${where}`, params);
    return { users: rows as DbUser[], total: Number((cnt as { n: number }[])[0]?.n ?? 0) };
  }

  // ---------- agents（管理后台用持久化注册表，实时状态仍以内存/Redis 为准） ----------

  async upsertAgent(a: { id: string; owner_id: string; name: string; platform: string | null; capabilities: string | null; status: string; last_ip?: string | null }): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      `INSERT INTO agents (id, owner_id, name, platform, capabilities, status, first_seen, last_seen, last_ip)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE owner_id = VALUES(owner_id), name = VALUES(name),
         platform = VALUES(platform), capabilities = VALUES(capabilities),
         status = VALUES(status), last_seen = VALUES(last_seen), last_ip = VALUES(last_ip)`,
      [a.id, a.owner_id, a.name, a.platform, a.capabilities, a.status, now, now, a.last_ip ?? null],
    );
  }

  async markAgentOffline(id: string): Promise<void> {
    await this.pool.query("UPDATE agents SET status = 'offline', last_seen = ? WHERE id = ?", [Date.now(), id]);
  }

  async touchAgent(id: string, status: string): Promise<void> {
    await this.pool.query("UPDATE agents SET status = ?, last_seen = ? WHERE id = ?", [status, Date.now(), id]);
  }

  async listAgentsPaged(opts: { ownerID?: string; status?: string; query?: string; limit: number; offset: number }): Promise<{ agents: DbAgent[]; total: number }> {
    const conds: string[] = [];
    const params: unknown[] = [];
    if (opts.ownerID) { conds.push("owner_id = ?"); params.push(opts.ownerID); }
    if (opts.status) { conds.push("status = ?"); params.push(opts.status); }
    if (opts.query) { conds.push("(id LIKE ? OR name LIKE ?)"); params.push(`%${opts.query}%`, `%${opts.query}%`); }
    const where = conds.length ? " WHERE " + conds.join(" AND ") : "";
    const [rows] = await this.pool.query(
      `SELECT * FROM agents${where} ORDER BY last_seen DESC LIMIT ? OFFSET ?`,
      [...params, opts.limit, opts.offset],
    );
    const [cnt] = await this.pool.query(`SELECT COUNT(*) AS n FROM agents${where}`, params);
    return { agents: rows as DbAgent[], total: Number((cnt as { n: number }[])[0]?.n ?? 0) };
  }

  async countAgents(): Promise<number> {
    const [rows] = await this.pool.query("SELECT COUNT(*) AS n FROM agents");
    return Number((rows as { n: number }[])[0]?.n ?? 0);
  }

  async reassignAgent(id: string, newOwnerID: string): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE agents SET owner_id = ? WHERE id = ?", [newOwnerID, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async getAgentRow(id: string): Promise<DbAgent | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM agents WHERE id = ?", [id]);
    return (rows as DbAgent[])[0];
  }

  // ---------- agent_brands（品牌目录）与注册审批 ----------

  async countBrands(): Promise<number> {
    const [rows] = await this.pool.query("SELECT COUNT(*) AS n FROM agent_brands");
    return Number((rows as { n: number }[])[0]?.n ?? 0);
  }

  async listBrands(): Promise<DbAgentBrand[]> {
    const [rows] = await this.pool.query("SELECT * FROM agent_brands ORDER BY created_at ASC");
    return rows as DbAgentBrand[];
  }

  async getBrandById(id: string): Promise<DbAgentBrand | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM agent_brands WHERE id = ?", [id]);
    return (rows as DbAgentBrand[])[0];
  }

  async getBrandByName(name: string): Promise<DbAgentBrand | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM agent_brands WHERE name = ?", [name]);
    return (rows as DbAgentBrand[])[0];
  }

  async createBrand(b: { id: string; name: string; description: string; logo_url: string | null; capabilities: string | null; launch_cmd: string | null; conn_type: string; endpoint: string | null }): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      "INSERT INTO agent_brands (id, name, description, logo_url, capabilities, launch_cmd, conn_type, endpoint, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [b.id, b.name, b.description, b.logo_url, b.capabilities, b.launch_cmd, b.conn_type, b.endpoint, now, now],
    );
  }

  async updateBrand(id: string, b: { name: string; description: string; logo_url: string | null; capabilities: string | null; launch_cmd: string | null; conn_type: string; endpoint: string | null; disabled: boolean }): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE agent_brands SET name = ?, description = ?, logo_url = ?, capabilities = ?, launch_cmd = ?, conn_type = ?, endpoint = ?, disabled = ?, updated_at = ? WHERE id = ?",
      [b.name, b.description, b.logo_url, b.capabilities, b.launch_cmd, b.conn_type, b.endpoint, b.disabled ? 1 : 0, Date.now(), id],
    );
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async deleteBrand(id: string): Promise<boolean> {
    const [res] = await this.pool.query("DELETE FROM agent_brands WHERE id = ?", [id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  // 页面上分配 agent 实例：选 connector + 品牌，admin 决策即已批准
  async assignAgent(a: { id: string; owner_id: string; name: string; brand_id: string; connector_id: string }): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      `INSERT INTO agents (id, owner_id, name, platform, capabilities, status, first_seen, last_seen, brand_id, connector_id, approval_status)
       VALUES (?, ?, ?, NULL, NULL, 'offline', ?, ?, ?, ?, 'approved')`,
      [a.id, a.owner_id, a.name, now, now, a.brand_id, a.connector_id],
    );
  }

  async listConnectorAgents(connectorID: string): Promise<DbAgent[]> {
    const [rows] = await this.pool.query(
      "SELECT * FROM agents WHERE connector_id = ? AND approval_status = 'approved' ORDER BY first_seen ASC", [connectorID]);
    return rows as DbAgent[];
  }

  async unassignAgent(id: string): Promise<boolean> {
    const [res] = await this.pool.query("DELETE FROM agents WHERE id = ?", [id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async setAgentApproval(id: string, status: "approved" | "pending" | "rejected"): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE agents SET approval_status = ? WHERE id = ?", [status, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  // client 主动注册（治理模式下）：建行待审批
  async createPendingAgent(a: { id: string; owner_id: string; name: string; brand_id: string | null }): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      `INSERT INTO agents (id, owner_id, name, platform, capabilities, status, first_seen, last_seen, brand_id, approval_status)
       VALUES (?, ?, ?, NULL, NULL, 'offline', ?, ?, ?, 'pending')`,
      [a.id, a.owner_id, a.name, now, now, a.brand_id],
    );
  }

  // ---------- device_keys ----------

  async createDeviceKey(k: { id: string; owner_id: string; name: string; key_hash: string }): Promise<void> {
    await this.pool.query(
      "INSERT INTO device_keys (id, owner_id, name, key_hash, created_at) VALUES (?, ?, ?, ?, ?)",
      [k.id, k.owner_id, k.name, k.key_hash, Date.now()],
    );
  }

  async listDeviceKeys(ownerID: string): Promise<DbDeviceKey[]> {
    const [rows] = await this.pool.query(
      "SELECT * FROM device_keys WHERE owner_id = ? ORDER BY created_at DESC", [ownerID]);
    return rows as DbDeviceKey[];
  }

  async getDeviceKeyByHash(hash: string): Promise<DbDeviceKey | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM device_keys WHERE key_hash = ?", [hash]);
    return (rows as DbDeviceKey[])[0];
  }

  async setDeviceKeyDisabled(id: string, disabled: boolean): Promise<boolean> {
    const [res] = await this.pool.query("UPDATE device_keys SET disabled = ? WHERE id = ?", [disabled ? 1 : 0, id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async touchDeviceKeyUsed(id: string): Promise<void> {
    await this.pool.query("UPDATE device_keys SET last_used_at = ? WHERE id = ?", [Date.now(), id]);
  }

  // ---------- pairing_codes（一次性接入码，owner 在生成时绑定） ----------

  async createPairingCode(c: { id: string; owner_id: string; code_hash: string; expires_at: number }): Promise<void> {
    await this.pool.query(
      "INSERT INTO pairing_codes (id, owner_id, code_hash, expires_at, created_at) VALUES (?, ?, ?, ?, ?)",
      [c.id, c.owner_id, c.code_hash, c.expires_at, Date.now()],
    );
  }

  async getPairingCodeByHash(hash: string): Promise<DbPairingCode | undefined> {
    const [rows] = await this.pool.query("SELECT * FROM pairing_codes WHERE code_hash = ?", [hash]);
    return (rows as DbPairingCode[])[0];
  }

  async listPairingCodes(ownerID: string): Promise<DbPairingCode[]> {
    const [rows] = await this.pool.query(
      "SELECT * FROM pairing_codes WHERE owner_id = ? ORDER BY created_at DESC", [ownerID]);
    return rows as DbPairingCode[];
  }

  async markPairingCodeUsed(id: string): Promise<void> {
    await this.pool.query("UPDATE pairing_codes SET used_at = ? WHERE id = ?", [Date.now(), id]);
  }

  async deletePairingCode(id: string): Promise<boolean> {
    const [res] = await this.pool.query("DELETE FROM pairing_codes WHERE id = ?", [id]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async listSessions(ownerID: string, agentID?: string): Promise<DbSession[]> {
    const select =
      "SELECT s.*, (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) AS message_count FROM sessions s";
    if (agentID) {
      const [rows] = await this.pool.query(
        select + " WHERE s.owner_id = ? AND s.agent_id = ? ORDER BY s.updated_at DESC",
        [ownerID, agentID],
      );
      return rows as DbSession[];
    }
    const [rows] = await this.pool.query(
      select + " WHERE s.owner_id = ? ORDER BY s.updated_at DESC",
      [ownerID],
    );
    return rows as DbSession[];
  }

  // 每个会话最后一条消息（owner 范围），用于 session.list 的 preview 摘要。
  // MySQL 8 窗口函数；content 交由上层解析截断
  async listLastMessages(ownerID: string): Promise<{ session_id: string; role: string; content: string }[]> {
    const [rows] = await this.pool.query(
      "SELECT session_id, role, content FROM (" +
      "  SELECT session_id, role, content," +
      "    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at DESC) AS rn" +
      "  FROM messages WHERE owner_id = ?" +
      ") t WHERE rn = 1",
      [ownerID],
    );
    return rows as { session_id: string; role: string; content: string }[];
  }

  async getSession(ownerID: string, id: string): Promise<DbSession | undefined> {
    const [rows] = await this.pool.query(
      "SELECT * FROM sessions WHERE id = ? AND owner_id = ?", [id, ownerID]);
    return (rows as DbSession[])[0];
  }

  async createSession(s: { id: string; owner_id: string; agent_id: string; title: string; workdir?: string | null }): Promise<DbSession> {
    const now = Date.now();
    await this.pool.query(
      "INSERT INTO sessions (id, owner_id, agent_id, title, workdir, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [s.id, s.owner_id, s.agent_id, s.title, s.workdir ?? null, now, now],
    );
    return { ...s, workdir: s.workdir ?? null, created_at: now, updated_at: now };
  }

  async renameSession(ownerID: string, id: string, title: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE sessions SET title = ?, updated_at = ? WHERE id = ? AND owner_id = ?",
      [title, Date.now(), id, ownerID],
    );
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  // 会话绑定/解绑工作目录（空串 = 清除）
  async setSessionWorkdir(ownerID: string, id: string, workdir: string | null): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE sessions SET workdir = ? WHERE id = ? AND owner_id = ?",
      [workdir, id, ownerID],
    );
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async touchSession(id: string, updatedAt: number): Promise<void> {
    await this.pool.query("UPDATE sessions SET updated_at = ? WHERE id = ?", [updatedAt, id]);
  }

  async deleteSession(ownerID: string, id: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "DELETE FROM sessions WHERE id = ? AND owner_id = ?", [id, ownerID]);
    if ((res as mysql.ResultSetHeader).affectedRows === 0) return false;
    await this.pool.query("DELETE FROM messages WHERE session_id = ?", [id]);
    return true;
  }

  // 保留策略用：找出更新时间早于 cutoff 的会话
  async listOldSessions(cutoffMs: number): Promise<DbSession[]> {
    const [rows] = await this.pool.query(
      "SELECT * FROM sessions WHERE updated_at < ? ORDER BY updated_at ASC LIMIT 1000", [cutoffMs]);
    return rows as DbSession[];
  }

  // before 为毫秒时间戳（游标分页：取早于该时间的 limit 条，按时间正序返回）
  async listMessages(ownerID: string, sessionID: string, limit: number, before?: number): Promise<DbMessage[]> {
    const params: unknown[] = [sessionID, ownerID];
    let sql = "SELECT * FROM (SELECT * FROM messages WHERE session_id = ? AND owner_id = ?";
    if (before !== undefined) {
      sql += " AND created_at < ?";
      params.push(before);
    }
    sql += " ORDER BY created_at DESC LIMIT ?) AS recent ORDER BY created_at ASC";
    params.push(limit);
    const [rows] = await this.pool.query(sql, params);
    return rows as DbMessage[];
  }

  async appendMessage(m: Omit<DbMessage, "created_at"> & { created_at?: number }): Promise<void> {
    await this.pool.query(
      "INSERT INTO messages (id, session_id, owner_id, agent_id, role, content, task_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [m.id, m.session_id, m.owner_id, m.agent_id, m.role, m.content, m.task_id, m.created_at ?? Date.now()],
    );
  }

  // 返回该会话消息总数（页面"加载更早"需要知道剩余量）
  async countMessages(ownerID: string, sessionID: string): Promise<number> {
    const [rows] = await this.pool.query(
      "SELECT COUNT(*) AS n FROM messages WHERE session_id = ? AND owner_id = ?",
      [sessionID, ownerID],
    );
    return Number((rows as { n: number }[])[0]?.n ?? 0);
  }

  // ---------- agent_groups（群组多 agent 沟通） ----------

  async createGroup(g: { id: string; owner_id: string; name: string; manager_agent_id: string | null }): Promise<DbGroup> {
    const now = Date.now();
    await this.pool.query(
      "INSERT INTO agent_groups (id, owner_id, name, manager_agent_id, created_at) VALUES (?, ?, ?, ?, ?)",
      [g.id, g.owner_id, g.name, g.manager_agent_id, now],
    );
    return { ...g, created_at: now };
  }

  async getGroup(ownerID: string, id: string): Promise<DbGroup | undefined> {
    const [rows] = await this.pool.query(
      "SELECT * FROM agent_groups WHERE id = ? AND owner_id = ?", [id, ownerID]);
    return (rows as DbGroup[])[0];
  }

  async listGroups(ownerID: string): Promise<DbGroup[]> {
    const [rows] = await this.pool.query(
      "SELECT * FROM agent_groups WHERE owner_id = ? ORDER BY created_at ASC", [ownerID]);
    return rows as DbGroup[];
  }

  async renameGroup(ownerID: string, id: string, name: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE agent_groups SET name = ? WHERE id = ? AND owner_id = ?", [name, id, ownerID]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async setGroupManager(ownerID: string, id: string, managerAgentID: string | null): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE agent_groups SET manager_agent_id = ? WHERE id = ? AND owner_id = ?", [managerAgentID, id, ownerID]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async deleteGroup(ownerID: string, id: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "DELETE FROM agent_groups WHERE id = ? AND owner_id = ?", [id, ownerID]);
    if ((res as mysql.ResultSetHeader).affectedRows === 0) return false;
    await this.pool.query("DELETE FROM agent_group_members WHERE group_id = ?", [id]);
    return true;
  }

  async addGroupMember(groupID: string, agentID: string): Promise<void> {
    await this.pool.query(
      "INSERT IGNORE INTO agent_group_members (group_id, agent_id, added_at) VALUES (?, ?, ?)",
      [groupID, agentID, Date.now()],
    );
  }

  async removeGroupMember(groupID: string, agentID: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "DELETE FROM agent_group_members WHERE group_id = ? AND agent_id = ?", [groupID, agentID]);
    return (res as mysql.ResultSetHeader).affectedRows > 0;
  }

  async listGroupMembers(groupID: string): Promise<string[]> {
    const [rows] = await this.pool.query(
      "SELECT agent_id FROM agent_group_members WHERE group_id = ? ORDER BY added_at ASC", [groupID]);
    return (rows as { agent_id: string }[]).map((r) => r.agent_id);
  }

  // ---------- agent_nicknames（用户对自有 agent 的备注名，仅展示用） ----------

  async setNickname(ownerID: string, agentID: string, nickname: string | null): Promise<void> {
    if (nickname === null || nickname === "") {
      await this.pool.query("DELETE FROM agent_nicknames WHERE owner_id = ? AND agent_id = ?", [ownerID, agentID]);
      return;
    }
    await this.pool.query(
      `INSERT INTO agent_nicknames (owner_id, agent_id, nickname, updated_at) VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE nickname = VALUES(nickname), updated_at = VALUES(updated_at)`,
      [ownerID, agentID, nickname, Date.now()],
    );
  }

  async listNicknamesForOwner(ownerID: string): Promise<Map<string, string>> {
    const [rows] = await this.pool.query(
      "SELECT agent_id, nickname FROM agent_nicknames WHERE owner_id = ?", [ownerID]);
    return new Map((rows as { agent_id: string; nickname: string }[]).map((r) => [r.agent_id, r.nickname]));
  }

  async listAllNicknames(): Promise<{ owner_id: string; agent_id: string; nickname: string }[]> {
    const [rows] = await this.pool.query("SELECT owner_id, agent_id, nickname FROM agent_nicknames");
    return rows as { owner_id: string; agent_id: string; nickname: string }[];
  }
}
