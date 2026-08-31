// 工行 AAM 统一认证适配（aam-sm-2.0.jar，com.icbc.ssic.*，对接文档 3.1/3.2 标准用法）：
//   登录页 → 网关调桥（空参数形态）→ SDK 内部 sendRedirect 到 AAM 授权页
//   → 用户凭办公软件状态自动放行 → 回跳 /auth/aam/callback?ssiAuth=...&ssiSign=...
//   → 网关再调桥（带参验签）：ServerSideAuthenticator 初始化（六 setter）
//     → execute() → request 属性 ssiCredentials → getSSICUser() → employeeNo
// 授权页 URL 由 SDK 内部构造（无需配置模板）。桥协议：stdin 一行 JSON → stdout 一行 JSON。

import { spawn } from "node:child_process";
import { logger } from "./util.ts";

export interface AAMConfig {
  server: string; // setServerName：AAM 服务器（如 aam.icbc）
  version: string; // setVersion：默认 SM2
  serviceName: string; // setServiceName：应用标识（AAM 注册时发）
  serviceURL: string; // setServiceURL：回调地址（留空则按请求 Host 推导）
  smPublicKey: string; // setSmPublicKey：SM2 公钥（JSON 串，注册时发）
  smKeyPass: string; // setSmKeyPass：公钥保护口令
  bridgeCmd: string; // 例：java -cp /opt/ywmatrix/aam:/opt/ywmatrix/aam/* AamBridge
}

export interface AAMIdentity {
  employeeNo: string; // 工号（SSICUserInfo.employeeNo；缺失时回退 username）
  username: string;
  name: string;
  department: string;
}

interface BridgeResult {
  ok: boolean;
  redirect?: string; // SDK sendRedirect 的地址（登录入口形态 = AAM 授权页）
  identity?: AAMIdentity;
  error?: string;
}

const BRIDGE_TIMEOUT_MS = 20_000; // 一次性 JVM 冷启动 + SM2 初始化，放宽些

// 极简 JSON 字符串抽取（桥输出一层平铺字段）
function jsonStringField(json: string, key: string): string | undefined {
  const m = new RegExp(`"${key}"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)"`).exec(json);
  if (!m) return undefined;
  try {
    return JSON.parse(`"${m[1]}"`) as string;
  } catch {
    return undefined;
  }
}

// 文档要求：ssiAuth/ssiSign 先去 \r\n，否则验签必败（网关与桥双侧都做）
function stripCrlf(s: string): string {
  return s.replaceAll(/[\r\n]/g, "");
}

// 一次性 Java 进程调桥：全部输入走 stdin（不进命令行，ps 看不到、无注入面）
async function runBridge(cfg: AAMConfig, serviceURL: string, ssiAuth: string, ssiSign: string): Promise<BridgeResult> {
  const cmd = cfg.bridgeCmd.trim();
  if (cmd === "") throw new Error("aam bridge command not configured");
  const argv = cmd.split(/\s+/).filter((s) => s !== "");
  const payload = JSON.stringify({
    serverName: cfg.server,
    version: cfg.version,
    serviceName: cfg.serviceName,
    serviceURL,
    smPublicKey: cfg.smPublicKey,
    smKeyPass: cfg.smKeyPass,
    ssiAuth: stripCrlf(ssiAuth),
    ssiSign: stripCrlf(ssiSign),
  });
  return new Promise<BridgeResult>((resolve, reject) => {
    const child = spawn(argv[0], argv.slice(1), { stdio: ["pipe", "pipe", "inherit"] });
    let out = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("aam bridge timeout"));
    }, BRIDGE_TIMEOUT_MS);
    timer.unref();
    child.stdout?.on("data", (c: Buffer) => { out += c.toString("utf8"); });
    child.on("error", (e) => {
      clearTimeout(timer);
      reject(new Error(`aam bridge spawn failed: ${e.message}`));
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      const line = out.split("\n").map((s) => s.trim()).filter((s) => s !== "").pop() ?? "";
      if (/"ok"\s*:\s*true/.test(line)) {
        const employeeNo = jsonStringField(line, "employeeNo") ?? "";
        const username = jsonStringField(line, "username") ?? employeeNo;
        const id = employeeNo !== "" ? employeeNo : username;
        if (id !== "") {
          resolve({
            ok: true,
            identity: {
              employeeNo: id,
              username,
              name: jsonStringField(line, "name") ?? jsonStringField(line, "fullName") ?? id,
              department: jsonStringField(line, "department") ?? "",
            },
          });
          return;
        }
        reject(new Error("bridge ok but no employeeNo/username in: " + line.slice(0, 200)));
        return;
      }
      resolve({ ok: false, error: jsonStringField(line, "error") ?? `exit ${code}`, redirect: jsonStringField(line, "redirect") });
    });
    child.stdin?.write(payload + "\n");
    child.stdin?.end();
    logger.debug("aam bridge invoked", { argv0: argv[0], auth_len: ssiAuth.length, sign_len: ssiSign.length });
  });
}

// 登录入口：空参数调桥，SDK 内部 sendRedirect 到 AAM 授权页，取该地址给浏览器 302
export async function startSignIn(cfg: AAMConfig, serviceURL: string): Promise<string> {
  const r = await runBridge(cfg, serviceURL, "", "");
  if (r.ok || !r.redirect || r.redirect === "") {
    throw new Error(r.ok ? "SDK 未发出跳转（登录入口形态异常）" : `aam login failed: ${r.error ?? "no redirect"}`);
  }
  return r.redirect;
}

// 回调验签：带 ssiAuth/ssiSign，成功返回工号身份
export async function verifySignIn(cfg: AAMConfig, serviceURL: string, ssiAuthRaw: string, ssiSignRaw: string): Promise<AAMIdentity> {
  if (ssiAuthRaw === "" || ssiSignRaw === "" || ssiAuthRaw.length > 8192 || ssiSignRaw.length > 8192) {
    throw new Error("invalid ssiAuth/ssiSign");
  }
  const r = await runBridge(cfg, serviceURL, ssiAuthRaw, ssiSignRaw);
  if (r.ok && r.identity) return r.identity;
  if (r.redirect && r.redirect !== "") {
    // 验签失败但 SDK 给了重登跳转（如会话失效）：透传给浏览器重走登录
    throw new RedirectError(r.redirect);
  }
  throw new Error(r.error ?? "aam verify failed");
}

// 验签失败但应重定向回 AAM 登录页（携带 redirect 属性的专用错误）
export class RedirectError extends Error {
  readonly redirect: string;
  constructor(redirect: string) {
    super("redirect:" + redirect);
    this.redirect = redirect;
  }
}
