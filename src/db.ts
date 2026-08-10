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
  agent_id: string;
  title: string;
  created_at: number;
  updated_at: number;
  message_count?: number;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sessions (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  title VARCHAR(256) NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  KEY idx_owner_agent (owner_id, agent_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS agents (
  id VARCHAR(128) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  name VARCHAR(256) NOT NULL,
  platform TEXT NULL,
  capabilities MEDIUMTEXT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'offline',
  first_seen BIGINT NOT NULL,
  last_seen BIGINT NOT NULL,
  KEY idx_owner (owner_id, last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS device_keys (
  id VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL,
  key_hash VARCHAR(128) NOT NULL UNIQUE,
  created_at BIGINT NOT NULL,
  last_used_at BIGINT NULL,
  disabled TINYINT(1) NOT NULL DEFAULT 0,
  KEY idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
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

  async upsertAgent(a: { id: string; owner_id: string; name: string; platform: string | null; capabilities: string | null; status: string }): Promise<void> {
    const now = Date.now();
    await this.pool.query(
      `INSERT INTO agents (id, owner_id, name, platform, capabilities, status, first_seen, last_seen)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE owner_id = VALUES(owner_id), name = VALUES(name),
         platform = VALUES(platform), capabilities = VALUES(capabilities),
         status = VALUES(status), last_seen = VALUES(last_seen)`,
      [a.id, a.owner_id, a.name, a.platform, a.capabilities, a.status, now, now],
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

  async getSession(ownerID: string, id: string): Promise<DbSession | undefined> {
    const [rows] = await this.pool.query(
      "SELECT * FROM sessions WHERE id = ? AND owner_id = ?", [id, ownerID]);
    return (rows as DbSession[])[0];
  }

  async createSession(s: { id: string; owner_id: string; agent_id: string; title: string }): Promise<DbSession> {
    const now = Date.now();
    await this.pool.query(
      "INSERT INTO sessions (id, owner_id, agent_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
      [s.id, s.owner_id, s.agent_id, s.title, now, now],
    );
    return { ...s, created_at: now, updated_at: now };
  }

  async renameSession(ownerID: string, id: string, title: string): Promise<boolean> {
    const [res] = await this.pool.query(
      "UPDATE sessions SET title = ?, updated_at = ? WHERE id = ? AND owner_id = ?",
      [title, Date.now(), id, ownerID],
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
}
