// HTTP/WS 服务器装配：配置解析（GatewayConfig / loadGatewayConfig）、登录与
// SSO（OIDC/AAM）、静态资源、附件与产品上传、WS 升级与心跳看护、createGatewayServer。
import http from "node:http";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import * as proto from "../protocol.ts";
import { envString, envDurationMs, parseDurationMs, parseFlags, logger } from "../util.ts";
import { Db, type DbUser } from "../db.ts";
import { Bus } from "../bus.ts";
import {
  type AttachmentStore,
  LocalAttachmentStore,
  createLocalAttachmentStore,
  createS3AttachmentStore,
  sanitizeFileName,
} from "../storage.ts";
import { hashPassword, verifyPassword, passwordNeedsRehash, signJwt, verifyJwt } from "../auth.ts";
import { startSignIn as aamStartSignIn, verifySignIn as aamVerifySignIn, RedirectError as AamRedirectError, type AAMConfig } from "../aam.ts";
import { OIDCProvider } from "../oidc.ts";
import { readTarEntry } from "../tar.ts";
import { clientIp, RateLimiter } from "../ratelimit.ts";
import { Hub, sendMsg, sendError } from "./hub.ts";
import type { AgentConn, UserConn, PendingPair } from "./types.ts";
import { handleUserMessage, handleAgentMessage } from "./dispatch.ts";
import {
  publishProductPackage, validateProductManifest, scanProductCatalog,
  productDirPath, productPackagePath, readProductMeta,
} from "./products.ts";
import { hashDeviceKey } from "./agents.ts";

// HTTP 管理员鉴权：Bearer JWT + admin 角色（产品上传/编辑/删除用）
export async function httpAdmin(req: http.IncomingMessage, cfg: GatewayConfig, db?: Db): Promise<DbUser | null> {
  const bearer = req.headers.authorization?.startsWith("Bearer ")
    ? req.headers.authorization.slice(7)
    : "";
  const claims = bearer ? verifyJwt(bearer, cfg.jwtSecret) : undefined;
  if (!claims || !db) return null;
  const user = await db.getUserById(claims.sub).catch(() => undefined);
  if (!user || user.disabled === 1 || user.role !== "admin") return null;
  return user;
}




// Watches a connection with periodic pings; terminates after consecutive
// missed pongs. 不再单轮 miss 即杀：系统睡眠唤醒时 ticker 先于对端补 pong 触发，
// 会把活连接误杀——连续 2 轮未回且距最近 pong 超 75s 才判死。
export function watchPong(ws: WebSocket, conn: { alive: boolean }, onPong?: () => void): NodeJS.Timeout {
  let lastPongAt = Date.now();
  let misses = 0;
  ws.on("pong", () => {
    conn.alive = true;
    misses = 0;
    lastPongAt = Date.now();
    onPong?.();
  });
  const ticker = setInterval(() => {
    if (conn.alive) {
      conn.alive = false;
      misses = 0;
      ws.ping();
      return;
    }
    misses++;
    if (misses >= 2 && Date.now() - lastPongAt > 75_000) {
      ws.terminate();
      return;
    }
    ws.ping();
  }, 30_000);
  ticker.unref();
  return ticker;
}

export interface GatewayConfig {
  addr: string;
  logLevel: string;
  agentTimeoutMs: number;
  userTimeoutMs: number;
  taskTimeoutMs: number;
  databaseURL: string;
  jwtSecret: string;
  jwtTtlMs: number;
  adminPassword: string;
  redisURL: string;
  redisPrefix: string;
  instanceID: string;
  trustProxy: boolean; // 前面有可信反代时才信任 X-Forwarded-For
  attachDir: string;
  productsDir?: string;
  attachQuotaMb: number;
  retentionDays: number;
  s3Endpoint: string;
  s3Region: string;
  s3Bucket: string;
  s3AccessKey: string;
  s3SecretKey: string;
  s3PublicURL: string;
  // OIDC 统一认证（四项全配才启用）
  oidcIssuer: string;
  oidcClientID: string;
  oidcClientSecret: string;
  oidcRedirectURL: string;
  oidcEmployeeClaim: string;
  // 工行 AAM 统一认证（aam-sm-2.0.jar；server/serviceName/smPublicKey/keyPass/bridge 五项全配才启用，见 package/aam/README.md）
  aamServer?: string; // AAM 服务器（setServerName，如 aam.icbc）
  aamVersion?: string; // 默认 SM2
  aamServiceName?: string; // 应用标识（setServiceName，AAM 注册时发）
  aamServiceURL?: string; // 回调地址（setServiceURL；留空按请求 Host 推导）
  aamSmPublicKey?: string; // SM2 公钥 JSON（注册时发）
  aamSmKeyPass?: string; // 公钥保护口令
  aamBridgeCmd?: string; // 例：java -cp /opt/ywmatrix/aam:/opt/ywmatrix/aam/* AamBridge
}

export function loadGatewayConfig(): GatewayConfig {
  const specs = [
    { name: "addr", type: "string" as const, default: envString("AGENT_MANAGE_ADDR", ":8080") },
    { name: "log-level", type: "string" as const, default: envString("AGENT_MANAGE_LOG_LEVEL", "info") },
    { name: "agent-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_AGENT_TIMEOUT", 90_000)) },
    { name: "user-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_USER_TIMEOUT", 120_000)) },
    { name: "task-timeout", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_TASK_TIMEOUT", 7_200_000)) },
    { name: "database-url", type: "string" as const, default: envString("AGENT_MANAGE_DATABASE_URL", "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix") },
    { name: "jwt-secret", type: "string" as const, default: envString("AGENT_MANAGE_JWT_SECRET", "") },
    { name: "jwt-ttl", type: "duration" as const, default: String(envDurationMs("AGENT_MANAGE_JWT_TTL", 7 * 86400_000)) },
    { name: "admin-password", type: "string" as const, default: envString("AGENT_MANAGE_ADMIN_PASSWORD", "admin123") },
    { name: "redis-url", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_URL", "") },
    { name: "redis-prefix", type: "string" as const, default: envString("AGENT_MANAGE_REDIS_PREFIX", "ywm") },
    { name: "instance-id", type: "string" as const, default: envString("AGENT_MANAGE_INSTANCE_ID", crypto.randomBytes(6).toString("hex")) },
    { name: "trust-proxy", type: "string" as const, default: envString("AGENT_MANAGE_TRUST_PROXY", "") },
    { name: "attach-dir", type: "string" as const, default: envString("AGENT_MANAGE_ATTACH_DIR", "data/attachments") },
    { name: "products-dir", type: "string" as const, default: envString("AGENT_MANAGE_PRODUCTS_DIR", "data/products") },
    { name: "attach-quota-mb", type: "string" as const, default: envString("AGENT_MANAGE_ATTACH_QUOTA_MB", "0") },
    { name: "retention-days", type: "string" as const, default: envString("AGENT_MANAGE_RETENTION_DAYS", "0") },
    { name: "s3-endpoint", type: "string" as const, default: envString("AGENT_MANAGE_S3_ENDPOINT", "") },
    { name: "s3-region", type: "string" as const, default: envString("AGENT_MANAGE_S3_REGION", "us-east-1") },
    { name: "s3-bucket", type: "string" as const, default: envString("AGENT_MANAGE_S3_BUCKET", "ywmatrix") },
    { name: "s3-access-key", type: "string" as const, default: envString("AGENT_MANAGE_S3_ACCESS_KEY", "minioadmin") },
    { name: "s3-secret-key", type: "string" as const, default: envString("AGENT_MANAGE_S3_SECRET_KEY", "minioadmin") },
    { name: "s3-public-url", type: "string" as const, default: envString("AGENT_MANAGE_S3_PUBLIC_URL", "") },
    { name: "oidc-issuer", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_ISSUER", "") },
    { name: "oidc-client-id", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_CLIENT_ID", "") },
    { name: "oidc-client-secret", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_CLIENT_SECRET", "") },
    { name: "oidc-redirect-url", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_REDIRECT_URL", "") },
    { name: "oidc-employee-claim", type: "string" as const, default: envString("AGENT_MANAGE_OIDC_EMPLOYEE_CLAIM", "employee_id") },
    { name: "aam-server", type: "string" as const, default: envString("AGENT_MANAGE_AAM_SERVER", "") },
    { name: "aam-version", type: "string" as const, default: envString("AGENT_MANAGE_AAM_VERSION", "SM2") },
    { name: "aam-service-name", type: "string" as const, default: envString("AGENT_MANAGE_AAM_SERVICE_NAME", "") },
    { name: "aam-service-url", type: "string" as const, default: envString("AGENT_MANAGE_AAM_SERVICE_URL", "") },
    { name: "aam-sm-public-key", type: "string" as const, default: envString("AGENT_MANAGE_AAM_SM_PUBLIC_KEY", "") },
    { name: "aam-sm-key-pass", type: "string" as const, default: envString("AGENT_MANAGE_AAM_SM_KEY_PASS", "") },
    { name: "aam-bridge-cmd", type: "string" as const, default: envString("AGENT_MANAGE_AAM_BRIDGE_CMD", "") },
  ];
  const values = parseFlags(specs);
  const toMs = (v: string, def: number): number => {
    const asNum = Number(v);
    if (v !== "" && !Number.isNaN(asNum)) return asNum;
    const parsed = parseDurationMs(v);
    return parsed !== undefined ? parsed : def;
  };
  return {
    addr: values["addr"],
    logLevel: values["log-level"],
    agentTimeoutMs: toMs(values["agent-timeout"], 90_000),
    userTimeoutMs: toMs(values["user-timeout"], 120_000),
    taskTimeoutMs: toMs(values["task-timeout"], 7_200_000),
    databaseURL: values["database-url"],
    jwtSecret: values["jwt-secret"],
    jwtTtlMs: toMs(values["jwt-ttl"], 7 * 86400_000),
    adminPassword: values["admin-password"],
    redisURL: values["redis-url"],
    redisPrefix: values["redis-prefix"],
    instanceID: values["instance-id"],
    trustProxy: /^(1|true|yes)$/i.test(values["trust-proxy"]),
    s3Endpoint: values["s3-endpoint"],
    s3Region: values["s3-region"],
    s3Bucket: values["s3-bucket"],
    s3AccessKey: values["s3-access-key"],
    s3SecretKey: values["s3-secret-key"],
    s3PublicURL: values["s3-public-url"],
    attachDir: values["attach-dir"],
    productsDir: values["products-dir"],
    attachQuotaMb: Number(values["attach-quota-mb"]) || 0,
    retentionDays: Number(values["retention-days"]) || 0,
    oidcIssuer: values["oidc-issuer"],
    oidcClientID: values["oidc-client-id"],
    oidcClientSecret: values["oidc-client-secret"],
    oidcRedirectURL: values["oidc-redirect-url"],
    oidcEmployeeClaim: values["oidc-employee-claim"],
    aamServer: values["aam-server"],
    aamVersion: values["aam-version"],
    aamServiceName: values["aam-service-name"],
    aamServiceURL: values["aam-service-url"],
    aamSmPublicKey: values["aam-sm-public-key"],
    aamSmKeyPass: values["aam-sm-key-pass"],
    aamBridgeCmd: values["aam-bridge-cmd"],
  };
}

// 统一认证（OIDC / 工行 AAM）共用收尾：按工号关联账号（首次自动建号）→ 签发 JWT
// → 回调页写 localStorage 跳转。已知登录类错误（存储未配置/账号禁用）经 fail 渲染
// 400 失败页；其余异常向上抛，由调用方兜底 500。
export async function ssoLoginResponse(
  hub: Hub, cfg: GatewayConfig, res: http.ServerResponse,
  employeeID: string, displayName: string, source: string,
  fail: (message: string) => void,
): Promise<void> {
  if (!hub.db) {
    fail("存储未配置");
    return;
  }
  let user = await hub.db.getUserByEmployeeID(employeeID);
  if (!user) {
    // name 与 employee_id 均有唯一约束，并发首次登录时撞哪边处理哪边：
    // 撞 employee_id 直接复用已建账号，撞 name 换后缀重试
    for (let attempt = 0; attempt < 5 && !user; attempt++) {
      let name = displayName;
      for (let i = 0; await hub.db.getUserByName(name); i++) {
        name = `${displayName}-${i + 2}`;
      }
      const candidate = {
        id: "u-" + crypto.randomUUID(),
        name,
        // 统一认证账号无本地密码：随机哈希占位，密码登录永远不匹配
        password_hash: await hashPassword(crypto.randomBytes(32).toString("hex")),
        role: "user",
        disabled: 0,
        created_at: Date.now(),
        last_login_at: null,
        employee_id: employeeID,
        display_name: displayName,
      };
      try {
        await hub.db.createUser(candidate);
        user = candidate;
        logger.info(source + " user provisioned", { user_id: candidate.id, name: candidate.name, employee_id: employeeID });
      } catch (e) {
        if ((e as { errno?: number }).errno !== 1062) throw e;
        user = await hub.db.getUserByEmployeeID(employeeID);
      }
    }
    if (!user) throw new Error(source + " user provisioning failed");
  }
  if (user.disabled === 1) {
    fail("账号已被禁用，请联系管理员");
    return;
  }
  hub.db.touchLastLogin(user.id).catch(() => {});
  const token = signJwt({ sub: user.id, name: user.name }, cfg.jwtSecret, cfg.jwtTtlMs);
  // 回调页与 SPA 同源，直接写 localStorage 后跳转（该页无 JS 回写，不会冲突）
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(`<!doctype html><meta charset="utf-8"><title>登录成功</title><p>登录成功，正在跳转…</p><script>
try {
  var s = JSON.parse(localStorage.getItem('agent_manage_v1') || '{}');
  s.token = ${JSON.stringify(token)};
  s.user = ${JSON.stringify({ id: user.id, name: user.name, role: user.role })};
  localStorage.setItem('agent_manage_v1', JSON.stringify(s));
  sessionStorage.removeItem('ywmAamBounce'); // 登录成功清 AAM 自动弹跳计数
} catch (e) {}
location.replace('/');
</script>`);
}

export function escapeHtmlText(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function readBody(req: http.IncomingMessage, limit = 64 * 1024): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > limit) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString()));
    req.on("error", reject);
  });
}

// 原始二进制体（产品包上传，上限远大于 JSON body）
export function readRawBody(req: http.IncomingMessage, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => {
      size += c.length;
      if (size > limit) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

export async function createGatewayServer(cfg: GatewayConfig, staticFile: string, db?: Db, attachments?: AttachmentStore) {
  const hub = new Hub(cfg.agentTimeoutMs, cfg.userTimeoutMs, cfg.taskTimeoutMs);
  hub.db = db;
  hub.attachments = attachments;
  if (db) {
    await hub.reloadBrands();
    // 启动恢复：编排任务态在内存，重启即丢。本实例残留的 running 行与超龄孤儿行
    // （实例消失等不到终态）统一兜底终结，管理者侧由客户端超时兜底，此处只做审计收口
    const staleBefore = Date.now() - Math.max(cfg.taskTimeoutMs * 2, 3_600_000);
    db.recoverRuns(cfg.instanceID, staleBefore, "gateway restarted")
      .then((n) => { if (n > 0) logger.warn("recovered stale orchestration runs", { count: n }); })
      .catch((e) => logger.error("orchestration run recovery failed", { error: String(e) }));
  }
  const productsDir = cfg.productsDir ?? "data/products"; // 产品分发目录（测试夹层不传时用默认）
  hub.productsDir = productsDir; // product.push 推送前用来校验包真实存在
  const loginLimiter = new RateLimiter(10, 60_000); // 每 IP 每分钟 10 次登录尝试
  const uploadLimiter = new RateLimiter(20, 60_000); // 每用户每分钟 20 次上传
  // 用户不存在时也跑一次 scrypt，拉齐登录接口时序，防用户名枚举
  const dummyPasswordHash = await hashPassword(crypto.randomBytes(16).toString("hex"));

  // OIDC 四项全配才启用；未启用时 /auth/oidc/* 返回 404
  const oidc = (cfg.oidcIssuer && cfg.oidcClientID && cfg.oidcClientSecret && cfg.oidcRedirectURL)
    ? new OIDCProvider({
        issuer: cfg.oidcIssuer,
        clientID: cfg.oidcClientID,
        clientSecret: cfg.oidcClientSecret,
        redirectURL: cfg.oidcRedirectURL,
        employeeClaim: cfg.oidcEmployeeClaim || "employee_id",
      }, cfg.jwtSecret)
    : undefined;
  if (oidc) logger.info("oidc enabled", { issuer: cfg.oidcIssuer, client_id: cfg.oidcClientID });
  // AAM 五项核心全配才启用；未启用时 /auth/aam/* 返回 404（跳转 URL 由 SDK 内部构造）
  const aam: AAMConfig | undefined = (cfg.aamServer && cfg.aamServiceName && cfg.aamSmPublicKey
    && cfg.aamSmKeyPass && cfg.aamBridgeCmd)
    ? {
        server: cfg.aamServer,
        version: cfg.aamVersion || "SM2",
        serviceName: cfg.aamServiceName,
        serviceURL: cfg.aamServiceURL ?? "",
        smPublicKey: cfg.aamSmPublicKey,
        smKeyPass: cfg.aamSmKeyPass,
        bridgeCmd: cfg.aamBridgeCmd,
      }
    : undefined;
  if (aam) logger.info("aam enabled", { server: aam.server, service: aam.serviceName });

  let bus: Bus | undefined;
  if (cfg.redisURL !== "") {
    bus = new Bus(cfg.redisURL, cfg.instanceID, cfg.agentTimeoutMs * 2, {
      onAgentMessage: (agentID, msg) => hub.deliverToLocalAgent(agentID, msg),
      onUserMessage: (ownerID, msg) => {
        hub.deliverToLocalUsers(ownerID, msg);
        // 任务终结通知可能跨实例到达，顺手清理本实例的任务计时器
        if (msg.method === proto.METHOD_ADMIN_PROGRESS) {
          const p = (msg.params ?? {}) as proto.AdminProgressParams;
          if (p.task_id && (p.done || (p.error !== undefined && p.error !== ""))) {
            hub.untrackTask(p.task_id);
          }
        }
      },
      onPendingResponse: (reqID, msg) => { hub.deliverToLocalPending(reqID, msg); },
      onAgentsChanged: () => hub.broadcastAgentList(),
      onKick: (userID, deviceKeyID, reason) => hub.kickLocal(userID, deviceKeyID, reason),
      onConnectorSync: (connectorID, msg) => hub.deliverToLocalConnector(connectorID, msg),
      onAgentApproval: (agentID, status) => hub.applyAgentApproval(agentID, status),
    }, cfg.redisPrefix);
    await bus.start();
    hub.bus = bus;
    logger.info("redis bus connected", { instance_id: cfg.instanceID });
  }

  // 静态文件内存缓存与 ETag：必须挂在请求 handler 外层（此前声明在每请求闭包里，
  // 每次请求都是新 Map，缓存从未生效——每个请求都读盘 + SHA1）。
  // 文件就几个，常驻缓存；重启进程即失效，无需失效机制。
  const fileCache = new Map<string, Buffer | null>();
  const readCached = (file: string, cb: (data: Buffer | null) => void) => {
    const hit = fileCache.get(file);
    if (hit !== undefined) {
      cb(hit);
      return;
    }
    fs.readFile(file, (err, data) => {
      fileCache.set(file, err ? null : data);
      cb(err ? null : data);
    });
  };
  // ETag（内容哈希，同内容跨重启稳定）+ If-None-Match → 304：
  // no-cache 策略下重复导航不再重传 HTML/CSS，只回"没变"
  const fileEtags = new Map<string, string>();
  const etagOf = (file: string, data: Buffer): string => {
    let et = fileEtags.get(file);
    if (!et) {
      et = `"${crypto.createHash("sha1").update(data).digest("base64url").slice(0, 20)}"`;
      fileEtags.set(file, et);
    }
    return et;
  };

  const server = http.createServer({
    // 显式超时，防慢速请求占住连接（与 Node 18+ 默认值一致，写死防升级漂移）
    headersTimeout: 60_000,
    requestTimeout: 300_000,
  }, (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/healthz" && req.method === "GET") {
      void (async () => {
        const dbOk = hub.draining ? "draining" : hub.db ? await hub.db.ping() : "disabled";
        const redisOk = hub.bus ? await hub.bus.ping() : "disabled";
        const healthy = !hub.draining && dbOk !== false && redisOk !== false;
        res.writeHead(healthy ? 200 : 503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: hub.draining ? "draining" : healthy ? "ok" : "degraded", db: dbOk, redis: redisOk, uptime_s: Math.floor(process.uptime()) }));
      })().catch(() => res.writeHead(503).end(JSON.stringify({ status: "error" })));
      return;
    }
    if (url.pathname === "/metrics" && req.method === "GET") {
      res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(hub.metrics.render([
        ["ywm_agents_connected", "Agents currently connected to this instance", hub.agents.size],
        ["ywm_users_connected", "Admin page connections on this instance", hub.users.size],
        ["ywm_tasks_active", "Tasks in flight", hub.tasks.size],
        ["ywm_pending_requests", "RPC requests awaiting agent response", hub.pendingRequests.size],
      ]));
      return;
    }
    if (url.pathname.startsWith("/files/") && req.method === "GET") {
      // 本地盘附件回源（URL 含 UUID，与 S3 匿名读策略同级）
      void (async () => {
        if (!(attachments instanceof LocalAttachmentStore)) {
          res.writeHead(404).end("not found");
          return;
        }
        const key = decodeURIComponent(url.pathname.slice("/files/".length));
        const obj = await attachments.get(key);
        if (!obj) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, {
          "Content-Type": obj.mime,
          "Content-Length": obj.body.length,
          "Cache-Control": "public, max-age=31536000, immutable",
          "Content-Security-Policy": "sandbox",
        });
        res.end(obj.body);
      })().catch(() => res.writeHead(500).end("internal error"));
      return;
    }
    // 静态文件缓存与 ETag 计算已提到 handler 外层（见 createServer 之前）
    const normEtag = (s: string): string => (s.startsWith("W/") ? s.slice(2) : s);
    const isNotModified = (file: string, data: Buffer): boolean => {
      const inm = req.headers["if-none-match"];
      return typeof inm === "string" && normEtag(inm) === normEtag(etagOf(file, data));
    };
    const serveHtml = (file: string) => {
      readCached(file, (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        const et = etagOf(file, data);
        if (isNotModified(file, data)) {
          res.writeHead(304, { ETag: et });
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache", ETag: et });
        res.end(data);
      });
    };
    if (url.pathname === "/") {
      serveHtml(staticFile);
      return;
    }
    if (url.pathname === "/admin" || url.pathname === "/admin.html") {
      // 管理后台是独立页面；鉴权在页面内由 JWT+role 完成
      serveHtml(path.resolve(path.dirname(staticFile), "admin.html"));
      return;
    }
    if (url.pathname === "/docs" || url.pathname === "/docs.html") {
      // 产品文档页（公开只读）
      serveHtml(path.resolve(path.dirname(staticFile), "docs.html"));
      return;
    }
    if (url.pathname.startsWith("/static/") && req.method === "GET") {
      // 共享静态资源（如 shared.css）；限制在 static 目录内防路径穿越
      const STATIC_MIME: Record<string, string> = { ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webmanifest": "application/manifest+json", ".webp": "image/webp", ".gif": "image/gif", ".ico": "image/x-icon", ".woff": "font/woff", ".woff2": "font/woff2" };
      const name = url.pathname.slice("/static/".length);
      const file = path.resolve(path.dirname(staticFile), name);
      if (name.includes("/") || !file.startsWith(path.dirname(staticFile) + path.sep)) {
        res.writeHead(404).end("not found");
        return;
      }
      readCached(file, (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        const et = etagOf(file, data);
        if (isNotModified(file, data)) {
          res.writeHead(304, { ETag: et });
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": STATIC_MIME[path.extname(file)] ?? "application/octet-stream", "Cache-Control": "no-cache", ETag: et });
        res.end(data);
      });
      return;
    }
    // PWA Service Worker 必须落在根 scope 才能控制整站，所以从根路径服务
    // static/sw.js，并用 Service-Worker-Allowed 放开 scope
    if (url.pathname === "/sw.js" && req.method === "GET") {
      readCached(path.resolve(path.dirname(staticFile), "sw.js"), (data) => {
        if (!data) {
          res.writeHead(404).end("not found");
          return;
        }
        res.writeHead(200, {
          "Content-Type": "text/javascript; charset=utf-8",
          "Cache-Control": "no-cache",
          "Service-Worker-Allowed": "/"
        });
        res.end(data);
      });
      return;
    }
    if (url.pathname === "/auth/config" && req.method === "GET") {
      // 登录页据此决定是否展示统一认证入口（AAM 优先于 OIDC，二者机制互斥展示一个入口）
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ oidc: oidc !== undefined, aam: aam !== undefined }));
      return;
    }
    if (url.pathname === "/auth/oidc/login" && req.method === "GET") {
      if (!oidc) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        const authURL = await oidc.buildAuthURL();
        res.writeHead(302, { Location: authURL });
        res.end();
      })().catch((e) => {
        logger.error("oidc login failed", { error: String(e) });
        res.writeHead(502, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("统一认证服务暂时不可用");
      });
      return;
    }
    if (url.pathname === "/auth/oidc/callback" && req.method === "GET") {
      if (!oidc) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        const fail = (message: string) => {
          res.writeHead(400, { "Content-Type": "text/html; charset=utf-8" });
          res.end(`<!doctype html><meta charset="utf-8"><title>登录失败</title><p>统一认证登录失败：${escapeHtmlText(message)}</p><p><a href="/">返回登录页</a></p>`);
        };
        const code = url.searchParams.get("code") ?? "";
        const state = url.searchParams.get("state") ?? "";
        if (!code || !state) {
          fail("缺少 code/state 参数");
          return;
        }
        if (!hub.db) {
          fail("存储未配置");
          return;
        }
        let identity;
        try {
          identity = await oidc.authenticate(code, state);
        } catch (e) {
          logger.warn("oidc authenticate failed", { error: String(e) });
          fail(e instanceof Error ? e.message : String(e));
          return;
        }
        await ssoLoginResponse(hub, cfg, res, identity.employeeID, identity.displayName, "oidc", fail);
      })().catch((e) => {
        logger.error("oidc callback failed", { error: String(e) });
        if (!res.headersSent) {
          res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
          res.end("internal error");
        }
      });
      return;
    }

    // ---- 工行 AAM 统一认证（CAS 式 ticket：跳转授权页 → 回调带 ticket → java 桥验签换工号） ----
    // ---- 工行 AAM 统一认证（ssiAuth/ssiSign 回跳参数 → Java 桥验签换工号） ----
    // 回调地址按请求 Host 推导（反代场景取 X-Forwarded-Host，trustProxy 已校验）
    const aamRequestBase = (): { proto: string; host: string } => {
      const host = cfg.trustProxy
        ? (Array.isArray(req.headers["x-forwarded-host"]) ? req.headers["x-forwarded-host"][0] : req.headers["x-forwarded-host"]) ?? req.headers.host
        : req.headers.host;
      const proto = cfg.trustProxy
        ? ((Array.isArray(req.headers["x-forwarded-proto"]) ? req.headers["x-forwarded-proto"][0] : req.headers["x-forwarded-proto"]) ?? "https")
        : "http";
      return { proto, host: host ?? "" };
    };

    if (url.pathname === "/auth/aam/login" && req.method === "GET") {
      if (!aam) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        // 空参数调桥：SDK 内部 sendRedirect 到 AAM 授权页（地址由 SDK 构造，不配置模板）
        const { proto, host } = aamRequestBase();
        const serviceURL = aam.serviceURL !== "" ? aam.serviceURL : `${proto}://${host}/auth/aam/callback`;
        const target = await aamStartSignIn(aam, serviceURL);
        res.writeHead(302, { Location: target });
        res.end();
      })().catch((e) => {
        logger.error("aam login failed", { error: String(e) });
        res.writeHead(502, { "Content-Type": "text/html; charset=utf-8" });
        res.end(`<!doctype html><meta charset="utf-8"><title>登录失败</title><p>统一认证服务暂时不可用：${escapeHtmlText(e instanceof Error ? e.message : String(e))}</p><p><a href="/">返回登录页</a></p>`);
      });
      return;
    }
    if (url.pathname === "/auth/aam/callback" && req.method === "GET") {
      if (!aam) {
        res.writeHead(404).end("not found");
        return;
      }
      void (async () => {
        const fail = (message: string) => {
          res.writeHead(400, { "Content-Type": "text/html; charset=utf-8" });
          res.end(`<!doctype html><meta charset="utf-8"><title>登录失败</title><p>统一认证登录失败：${escapeHtmlText(message)}</p><p><a href="/">返回登录页</a></p>`);
        };
        // AAM 回跳携带 ssiAuth / ssiSign（对接文档 3.2：来自回跳 URL 的 query 参数）
        const ssiAuth = url.searchParams.get("ssiAuth") ?? "";
        const ssiSign = url.searchParams.get("ssiSign") ?? "";
        if (!ssiAuth || !ssiSign) {
          fail("缺少 ssiAuth/ssiSign 参数");
          return;
        }
        if (!hub.db) {
          fail("存储未配置");
          return;
        }
        // setServiceURL：显式配置优先，否则按本请求 Host 推导（与注册到 AAM 的回调一致）
        const { proto, host } = aamRequestBase();
        const serviceURL = aam.serviceURL !== "" ? aam.serviceURL : `${proto}://${host}/auth/aam/callback`;
        let identity;
        try {
          identity = await aamVerifySignIn(aam, serviceURL, ssiAuth, ssiSign);
        } catch (e) {
          if (e instanceof AamRedirectError) {
            // 验签失败但 SDK 指了重登地址（如会话失效）：带浏览器重走
            res.writeHead(302, { Location: e.redirect });
            res.end();
            return;
          }
          logger.warn("aam verify failed", { error: String(e) });
          fail(e instanceof Error ? e.message : String(e));
          return;
        }
        // 工号（employeeNo）关联账号，姓名做显示名
        await ssoLoginResponse(hub, cfg, res, identity.employeeNo, identity.name || identity.username, "aam", fail);
      })().catch((e) => {
        logger.error("aam callback failed", { error: String(e) });
        if (!res.headersSent) {
          res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
          res.end("internal error");
        }
      });
      return;
    }
    if (url.pathname === "/auth/login" && req.method === "POST") {
      void (async () => {
        const ip = clientIp(req.headers, req.socket.remoteAddress, cfg.trustProxy);
        if (!loginLimiter.allow(ip)) {
          res.writeHead(429, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "too many login attempts, try again later" }));
          return;
        }
        if (!db) {
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "storage not configured" }));
          return;
        }
        let body: { name?: string; password?: string };
        try {
          body = JSON.parse(await readBody(req)) as typeof body;
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid json" }));
          return;
        }
        const fail = () => {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid credentials" }));
        };
        const user = body.name ? await db.getUserByName(body.name) : undefined;
        // 无论用户是否存在都执行一次 scrypt，拉齐响应时序（|| 短路会前功尽弃）
        const passwordOk = await verifyPassword(body.password ?? "", user?.password_hash ?? dummyPasswordHash);
        if (!user || user.disabled === 1 || !body.password || !passwordOk) {
          fail();
          return;
        }
        // 旧格式/弱参数哈希在登录成功后静默升级为当前参数（不影响登录结果）
        if (passwordNeedsRehash(user.password_hash)) {
          const upgraded = await hashPassword(body.password);
          db.setUserPassword(user.id, upgraded).catch((e) =>
            logger.warn("password rehash failed", { user: user.id, error: String(e) }));
        }
        const token = signJwt({ sub: user.id, name: user.name }, cfg.jwtSecret, cfg.jwtTtlMs);
        db.touchLastLogin(user.id).catch((e) => logger.warn("touch last login failed", { error: String(e) }));
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ token, user: { id: user.id, name: user.name, role: user.role } }));
      })().catch((e) => {
        logger.error("login failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }
    // 轻量 token 校验：前端启动时先验一次，失效 token 直接清掉停在登录页，
    // 避免乐观进主界面后 WS 三次 401 重试又弹回登录页的闪进闪回
    if (url.pathname === "/auth/me" && req.method === "GET") {
      void (async () => {
        const bearer = req.headers.authorization?.startsWith("Bearer ")
          ? req.headers.authorization.slice(7)
          : "";
        const claims = bearer ? verifyJwt(bearer, cfg.jwtSecret) : undefined;
        const user = claims && db ? await db.getUserById(claims.sub).catch(() => undefined) : undefined;
        if (!claims || !user || user.disabled === 1) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ user: { id: user.id, name: user.name, role: user.role } }));
      })().catch((e) => {
        logger.error("auth/me failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }
    if (url.pathname === "/attachments" && req.method === "POST") {
      void (async () => {
        if (!attachments) {
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "attachment store not configured" }));
          return;
        }
        // 仅接受 Authorization 头；URL query 传 token 会被反代日志/浏览器历史记录
        const bearer = req.headers.authorization?.startsWith("Bearer ")
          ? req.headers.authorization.slice(7)
          : "";
        const claims = bearer ? verifyJwt(bearer, cfg.jwtSecret) : undefined;
        if (!claims) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        if (!uploadLimiter.allow(claims.sub)) {
          res.writeHead(429, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "upload too frequent" }));
          return;
        }
        let body: { name?: string; mime?: string; data?: string };
        try {
          body = JSON.parse(await readBody(req, 32 * 1024 * 1024)) as typeof body;
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "invalid json or body too large" }));
          return;
        }
        if (!body.data) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "missing data" }));
          return;
        }
        const b64 = body.data.includes(",") ? body.data.slice(body.data.indexOf(",") + 1) : body.data;
        const buf = Buffer.from(b64, "base64");
        if (buf.length === 0 || buf.length > 20 * 1024 * 1024) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "empty or exceeds 20MB" }));
          return;
        }
        // 按用户配额（仅本地盘模式；S3 模式由 bucket 策略/生命周期管理）
        if (cfg.attachQuotaMb > 0 && attachments instanceof LocalAttachmentStore) {
          const used = await attachments.usage(`attachments/${claims.sub}`);
          if (used + buf.length > cfg.attachQuotaMb * 1024 * 1024) {
            res.writeHead(429, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: "attachment quota exceeded" }));
            return;
          }
        }
        const mime = body.mime ?? "application/octet-stream";
        const key = `attachments/${claims.sub}/${crypto.randomUUID()}/${sanitizeFileName(body.name ?? "file")}`;
        const fileUrl = await attachments.put(key, buf, mime);
        hub.metrics.inc("ywm_attachments_uploaded_total");
        hub.metrics.inc("ywm_attachment_bytes_total", buf.length);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ url: fileUrl, name: body.name ?? "file", mime, size: buf.length }));
      })().catch((e) => {
        logger.error("attachment upload failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }

    /* ---------- 产品分发：目录 / 下载 / 上传（admin） / manifest 编辑（admin） / 删除（admin） ---------- */

    if (url.pathname === "/products/catalog" && req.method === "GET") {
      void (async () => {
        try {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ products: scanProductCatalog(productsDir) }));
        } catch (e) {
          logger.error("product catalog failed", { error: String(e) });
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "internal error" }));
        }
      })();
      return;
    }

    const dl = /^\/products\/([A-Za-z0-9._-]+)\/((?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?)\/download$/.exec(url.pathname);
    if (dl && req.method === "GET") {
      const file = productPackagePath(productsDir, dl[1], dl[2]);
      if (!file || !fs.existsSync(file)) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "product not found" }));
        return;
      }
      const meta = readProductMeta(path.dirname(file));
      if (meta) res.setHeader("X-Checksum-Sha256", meta.sha256);
      res.writeHead(200, { "Content-Type": "application/gzip" });
      fs.createReadStream(file).pipe(res);
      return;
    }

    if (url.pathname === "/products/upload" && req.method === "POST") {
      void (async () => {
        const admin = await httpAdmin(req, cfg, db);
        if (!admin) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        const filename = url.searchParams.get("filename") || "package.tgz";
        if (!/\.(tar\.gz|tgz)$/i.test(filename)) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "仅支持 .tar.gz / .tgz 安装包" }));
          return;
        }
        const buf = await readRawBody(req, 512 * 1024 * 1024);
        try {
          const r = await publishProductPackage(productsDir, buf, filename, db);
          logger.info("product published", { by: admin.name, brand: r.brand, version: r.version, sha256: r.sha256.slice(0, 12) + "…" });
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(r));
        } catch (e) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: e instanceof Error ? e.message : String(e) }));
        }
      })().catch((e) => {
        logger.error("product upload failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }

    const mpm = /^\/products\/([A-Za-z0-9._-]+)\/((?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?)\/manifest$/.exec(url.pathname);
    if (mpm && req.method === "PUT") {
      void (async () => {
        const admin = await httpAdmin(req, cfg, db);
        if (!admin) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        const dir = productDirPath(productsDir, mpm[1], mpm[2]);
        if (!dir || !fs.existsSync(path.join(dir, "manifest.json"))) {
          res.writeHead(404, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "product not found" }));
          return;
        }
        try {
          const body = JSON.parse(await readBody(req, 1024 * 1024)) as Record<string, unknown>;
          const m = validateProductManifest(body);
          if (m.brand !== mpm[1] || m.version !== mpm[2]) {
            throw new Error("brand/version 是目录身份，不可修改（要换身份请重新上传包）");
          }
          fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(m, null, 2), "utf8");
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "ok", manifest: m }));
        } catch (e) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: e instanceof Error ? e.message : String(e) }));
        }
      })().catch((e) => {
        logger.error("product manifest update failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }

    const mpd = /^\/products\/([A-Za-z0-9._-]+)\/((?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?)$/.exec(url.pathname);
    if (mpd && req.method === "DELETE") {
      void (async () => {
        const admin = await httpAdmin(req, cfg, db);
        if (!admin) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "unauthorized" }));
          return;
        }
        const dir = productDirPath(productsDir, mpd[1], mpd[2]);
        if (!dir || !fs.existsSync(dir)) {
          res.writeHead(404, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "product not found" }));
          return;
        }
        fs.rmSync(dir, { recursive: true, force: true });
        logger.info("product removed", { by: admin.name, brand: mpd[1], version: mpd[2] });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "ok" }));
      })().catch((e) => {
        logger.error("product delete failed", { error: String(e) });
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "internal error" }));
      });
      return;
    }

    res.writeHead(404).end("not found");
  });

  // maxPayload 限制单帧大小（ws 默认 100MiB 太大）；附件走 HTTP，WS 上都是 JSON 控制消息
  const wss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });

  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const token = url.searchParams.get("token") ?? "";

    if (url.pathname !== "/ws/agent" && url.pathname !== "/ws/admin") {
      socket.destroy();
      return;
    }
    if (hub.draining) {
      socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
      socket.destroy();
      return;
    }
    const unauthorized = (): void => {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
    };
    // /ws/admin 仅接受 JWT；/ws/agent 在无 token 时接受设备密钥（?key=）或配对模式（?pair=1）
    const deviceKey = url.pathname === "/ws/agent" && token === ""
      ? url.searchParams.get("key") ?? ""
      : "";
    const pairing = url.pathname === "/ws/agent" && token === "" && deviceKey === ""
      && url.searchParams.get("pair") === "1";

    // query 凭证与首帧凭证共用的解析逻辑
    const resolveCreds = async (t: string, k: string): Promise<
      { userID: string; deviceKeyID?: string; isAdmin: boolean } | undefined
    > => {
      let userID = "";
      let deviceKeyID: string | undefined;
      if (t !== "") {
        const claims = verifyJwt(t, cfg.jwtSecret);
        if (!claims) return undefined;
        userID = claims.sub;
      } else if (k !== "" && url.pathname === "/ws/agent") {
        // 设备密钥认证：未知/禁用统一拒绝，防探测
        if (!hub.db) return undefined;
        const key = await hub.db.getDeviceKeyByHash(hashDeviceKey(k)).catch(() => undefined);
        if (!key || key.disabled === 1) return undefined;
        userID = key.owner_id;
        deviceKeyID = key.id;
      } else {
        return undefined;
      }
      // 禁用账号即时生效（JWT 未过期也拒绝新连接）；顺带取 admin 角色
      if (hub.db) {
        const u = await hub.db.getUserById(userID).catch(() => undefined);
        if (!u || u.disabled === 1) return undefined;
        return { userID, deviceKeyID, isAdmin: u.role === "admin" };
      }
      return { userID, deviceKeyID, isAdmin: false };
    };

    // 认证通过后建立连接（query 路径与首帧路径共用）
    const establish = (ws: WebSocket, userID: string, deviceKeyID: string | undefined, isAdmin: boolean): void => {
      const ip = clientIp(req.headers, req.socket.remoteAddress, cfg.trustProxy);
      if (url.pathname === "/ws/agent") {
        const agent: AgentConn = {
          id: "",
          ownerID: userID,
          name: "",
          ws,
          capabilities: [],
          status: proto.AGENT_STATUS_ONLINE,
          lastHeartbeat: Date.now(),
          alive: true,
          deviceKeyID,
          ip,
          pairing: false,
        };
        const ticker = watchPong(ws, agent, () => {
          // pong = 连接活着：connector 模式一条 ws 托管多 agent，全部续命
          hub.livenessProbes.delete(ws);
          for (const a of hub.agents.values()) {
            if (a.ws === ws) a.lastHeartbeat = Date.now();
          }
        });
        ws.on("message", (data) => handleAgentMessage(hub, agent, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          hub.livenessProbes.delete(ws);
          // 一条连接可能托管多个 agent（connector 模式）：按 ws 全部注销
          const ids = [...hub.agents.values()].filter((a) => a.ws === ws).map((a) => a.id);
          for (const id of ids) hub.unregisterAgent(id);
          if (agent.connectorID) hub.unregisterConnector(agent.connectorID, ws);
        });
        ws.on("error", () => ws.close());
      } else {
        const user: UserConn = {
          ws, userID, lastHeartbeat: Date.now(), alive: true, isAdmin,
          ownOnly: url.searchParams.get("scope") === "own",
        };
        hub.registerUser(user);
        const ticker = watchPong(ws, user, () => {
          hub.livenessProbes.delete(ws);
          user.lastHeartbeat = Date.now();
        });
        ws.on("message", (data) => handleUserMessage(hub, user, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          hub.livenessProbes.delete(ws);
          hub.unregisterUser(ws);
        });
        ws.on("error", () => ws.close());
      }
    };

    // 无凭证配对连接（?pair=1）：只允许 connector.pair，审批下发密钥后重连
    if (pairing) {
      wss.handleUpgrade(req, socket, head, (ws) => {
        const agent: AgentConn = {
          id: "",
          ownerID: "",
          name: "",
          ws,
          capabilities: [],
          status: proto.AGENT_STATUS_ONLINE,
          lastHeartbeat: Date.now(),
          alive: true,
          ip: clientIp(req.headers, req.socket.remoteAddress, cfg.trustProxy),
          pairing: true,
        };
        const ticker = watchPong(ws, agent, () => {
          hub.livenessProbes.delete(ws);
        });
        ws.on("message", (data) => handleAgentMessage(hub, agent, data.toString()));
        ws.on("close", () => {
          clearInterval(ticker);
          hub.livenessProbes.delete(ws);
          // 配对挂起连接断开：移出待接入列表
          for (const [cid, p] of hub.pendingPairs) {
            if (p.conn.ws === ws) hub.pendingPairs.delete(cid);
          }
        });
        ws.on("error", () => ws.close());
      });
      return;
    }

    // 首帧认证：无 query 凭证时也接受升级，连接挂起等第一条 auth 消息。
    // 凭证不再进 URL（反代 access log / 浏览器历史是真实泄露面）；
    // 10s 未完成认证即断开。query 路径（?token=/?key=）保留兼容存量终端。
    if (token === "" && deviceKey === "") {
      wss.handleUpgrade(req, socket, head, (ws) => {
        let authed = false;
        const timer = setTimeout(() => {
          if (!authed) ws.close(4001, "auth timeout");
        }, proto.AUTH_TIMEOUT_MS);
        timer.unref();
        ws.on("message", (data) => {
          if (authed) return;
          let msg: proto.Message;
          try {
            msg = JSON.parse(data.toString()) as proto.Message;
          } catch {
            ws.close(4001, "malformed message");
            return;
          }
          if (msg.method !== proto.METHOD_AUTH) {
            sendError(ws, msg.id, proto.ERR_UNAUTHORIZED, "first message must be auth");
            ws.close(4001, "auth required");
            return;
          }
          const params = (msg.params ?? {}) as proto.AuthParams;
          void resolveCreds(String(params.token ?? ""), String(params.key ?? "")).then((identity) => {
            if (authed) return;
            if (!identity) {
              sendError(ws, msg.id, proto.ERR_UNAUTHORIZED, "invalid credentials");
              ws.close(4001, "invalid credentials");
              return;
            }
            authed = true;
            clearTimeout(timer);
            if (identity.deviceKeyID !== undefined && hub.db) {
              hub.db.touchDeviceKeyUsed(identity.deviceKeyID).catch(() => {});
            }
            sendMsg(ws, proto.newResponse(msg.id ?? "", { status: "ok" }));
            establish(ws, identity.userID, identity.deviceKeyID, identity.isAdmin);
          }).catch(() => ws.close(4001, "auth failed"));
        });
        ws.on("error", () => ws.close());
      });
      return;
    }

    // query 凭证路径（兼容存量终端）
    void (async () => {
      const identity = await resolveCreds(token, deviceKey);
      if (!identity) {
        unauthorized();
        return;
      }
      if (identity.deviceKeyID !== undefined && hub.db) {
        hub.db.touchDeviceKeyUsed(identity.deviceKeyID).catch(() => {});
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        establish(ws, identity.userID, identity.deviceKeyID, identity.isAdmin);
      });
    })().catch(() => socket.destroy());
  });

  return { server, hub, wss, bus };
}

