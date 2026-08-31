// 进程入口：jwt secret 文件、生产配置检查、DB/admin 初始化、附件存储、保留策略、
// 监听与优雅关闭。由 src/gateway.ts 在作为主模块执行时调用。
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { logger, setLogLevel, parseListenAddr } from "../util.ts";
import { Db } from "../db.ts";
import {
  type AttachmentStore,
  createLocalAttachmentStore,
  createS3AttachmentStore,
} from "../storage.ts";
import { hashPassword } from "../auth.ts";
import { createGatewayServer, loadGatewayConfig } from "./http.ts";
import { deleteSessionWithAttachments } from "./sessions.ts";

export const JWT_SECRET_FILE = path.resolve("data/jwt-secret");

export async function loadOrCreateJwtSecretFile(): Promise<{ secret: string; source: "loaded" | "generated" }> {
  await fsp.mkdir(path.dirname(JWT_SECRET_FILE), { recursive: true });
  try {
    const raw = await fsp.readFile(JWT_SECRET_FILE, "utf8");
    const secret = raw.trim();
    if (secret.length >= 32) return { secret, source: "loaded" };
    logger.warn("data/jwt-secret 内容过短或损坏，重新生成", { len: secret.length });
  } catch (e) {
    // 文件不存在时落入下方生成分支
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  const secret = crypto.randomBytes(32).toString("hex");
  await fsp.writeFile(JWT_SECRET_FILE, secret, { mode: 0o600 });
  return { secret, source: "generated" };
}

export async function rotateJwtSecret(): Promise<void> {
  if (process.env.AGENT_MANAGE_JWT_SECRET && process.env.AGENT_MANAGE_JWT_SECRET !== "") {
    logger.error("已通过 AGENT_MANAGE_JWT_SECRET 环境变量提供 secret，请改环境变量来轮换；文件方式不生效");
    process.exit(1);
  }
  await fsp.mkdir(path.dirname(JWT_SECRET_FILE), { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "").slice(0, 15);
  try {
    await fsp.copyFile(JWT_SECRET_FILE, `${JWT_SECRET_FILE}.${stamp}.bak`);
    logger.info("已备份旧 secret", { backup: `${JWT_SECRET_FILE}.${stamp}.bak` });
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  const fresh = crypto.randomBytes(32).toString("hex");
  await fsp.writeFile(JWT_SECRET_FILE, fresh, { mode: 0o600 });
  logger.info("已写入新 secret", { file: JWT_SECRET_FILE });
  logger.warn("重启 gateway 后新 secret 生效；届时所有现有 token 失效，用户需重新登录、Agent 需重连");
}


export async function main(staticFile: string): Promise<void> {
  const cfg = loadGatewayConfig();
  setLogLevel(cfg.logLevel);
  // 生产环境（NODE_ENV=production）对弱默认值 fail-fast：
  // 默认库凭据/默认 admin 密码/默认 S3 密钥/过短 JWT secret 直接拒启，
  // 避免"按默认值静默连上开发库 / admin123 挂在公网"。开发环境保持原行为。
  if (process.env.NODE_ENV === "production") {
    const problems: string[] = [];
    if (cfg.databaseURL === "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix") {
      problems.push("-database-url 仍是开发默认值（AGENT_MANAGE_DATABASE_URL）");
    }
    if (cfg.adminPassword === "admin123") {
      problems.push("-admin-password 仍是 admin123（AGENT_MANAGE_ADMIN_PASSWORD）");
    }
    if (cfg.s3Endpoint && (cfg.s3AccessKey === "minioadmin" || cfg.s3SecretKey === "minioadmin")) {
      problems.push("S3 密钥仍是 minioadmin（AGENT_MANAGE_S3_ACCESS_KEY / _SECRET_KEY）");
    }
    if (cfg.jwtSecret !== "" && cfg.jwtSecret.length < 32) {
      problems.push("-jwt-secret 长度 < 32（AGENT_MANAGE_JWT_SECRET，建议 >= 随机 32 字节 hex）");
    }
    if (problems.length > 0) {
      logger.error("生产环境配置检查未通过，拒绝启动：\n  " + problems.join("\n  "));
      process.exit(1);
    }
  } else if (cfg.jwtSecret !== "" && cfg.jwtSecret.length < 32) {
    logger.warn("jwt-secret 长度 < 32，建议使用随机 32 字节 hex");
  }
  if (cfg.jwtSecret === "") {
    if (process.env.AGENT_MANAGE_JWT_SECRET && process.env.AGENT_MANAGE_JWT_SECRET !== "") {
      // 理论上 loadGatewayConfig 已注入；保险一行
      cfg.jwtSecret = process.env.AGENT_MANAGE_JWT_SECRET;
    } else {
      const r = await loadOrCreateJwtSecretFile();
      cfg.jwtSecret = r.secret;
      if (r.source === "loaded") {
        logger.info("jwt secret 已从 data/jwt-secret 加载（重启后 token 仍然有效）");
      } else {
        logger.warn("已生成新 jwt secret 并保存到 data/jwt-secret（后续重启会复用此 secret）");
      }
    }
  }
  const db = new Db(cfg.databaseURL);
  await db.init();
  logger.info("database connected", { url: cfg.databaseURL.replace(/:\/\/[^@]*@/, "://***@") });
  const admin = await db.getUserByName("admin");
  if (!admin) {
    await db.createUser({
      id: crypto.randomUUID(),
      name: "admin",
      password_hash: await hashPassword(cfg.adminPassword),
      role: "admin",
    });
    logger.warn("已创建初始 admin 账号，请尽快修改默认密码（AGENT_MANAGE_ADMIN_PASSWORD）");
  }
  let attachments: AttachmentStore | undefined;
  if (cfg.s3Endpoint !== "") {
    attachments = await createS3AttachmentStore({
      endpoint: cfg.s3Endpoint,
      region: cfg.s3Region,
      bucket: cfg.s3Bucket,
      accessKey: cfg.s3AccessKey,
      secretKey: cfg.s3SecretKey,
      publicURLBase: cfg.s3PublicURL !== "" ? cfg.s3PublicURL : undefined,
    });
    if (attachments) logger.info("attachment store ready (s3)", { endpoint: cfg.s3Endpoint, bucket: cfg.s3Bucket });
  } else if (cfg.attachDir !== "") {
    attachments = await createLocalAttachmentStore(path.resolve(cfg.attachDir));
    if (attachments) logger.info("attachment store ready (local)", { dir: path.resolve(cfg.attachDir) });
  }
  const { server, hub } = await createGatewayServer(cfg, staticFile, db, attachments);
  // 保留策略：每天清理 updated_at 早于 retentionDays 的会话（级联消息与附件）
  if (cfg.retentionDays > 0) {
    const purge = async () => {
      const cutoff = Date.now() - cfg.retentionDays * 86400_000;
      const old = await db.listOldSessions(cutoff);
      for (const s of old) {
        await deleteSessionWithAttachments(hub, s.owner_id, s.id);
      }
      if (old.length) logger.info("retention purge", { sessions: old.length, retention_days: cfg.retentionDays });
    };
    void purge().catch((e) => logger.error("retention purge failed", { error: String(e) }));
    setInterval(() => {
      void purge().catch((e) => logger.error("retention purge failed", { error: String(e) }));
    }, 86400_000).unref();
    logger.info("retention policy enabled", { days: cfg.retentionDays });
  }
  const { host, port } = parseListenAddr(cfg.addr);
  server.listen(port, host, () => {
    logger.info("gateway listening", { addr: cfg.addr });
  });

  // 优雅关闭：healthz 立即 503 供 LB 摘流，断 WS 让 client 重连到其他实例，
  // 留短宽限给消息落库与 close 握手，随后停止接受新连接并关 Redis/MySQL 退出
  let stopping = false;
  const shutdown = (signal: string) => {
    if (stopping) return;
    stopping = true;
    logger.info("shutting down", { signal });
    hub.shutdown();
    const force = setTimeout(() => process.exit(1), 10_000);
    force.unref();
    setTimeout(() => {
      void (async () => {
        server.close();
        server.closeAllConnections();
        await hub.bus?.stop().catch((e) => logger.error("bus stop failed", { error: String(e) }));
        await db.close().catch((e) => logger.error("db close failed", { error: String(e) }));
        logger.info("shutdown complete");
        process.exit(0);
      })();
    }, 1500);
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}
