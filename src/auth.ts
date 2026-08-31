// JWT (HS256) 签发/校验与 scrypt 密码哈希，全部基于 node:crypto，无外部依赖。

import crypto from "node:crypto";

// scrypt 参数（OWASP Password Storage Cheat Sheet：N≥2^17, r=8, p=1）。
// 存储格式带参数版本号：scrypt2:N:r:p:<salt-hex>:<hash-hex>，
// 将来提高强度只需改这里的默认值 + 密码NeedsRehash 旧条目在登录时自动升级。
// 旧格式 scrypt:<salt>:<hash>（Node 默认参数 N=16384）仅验证，不再签发。
const SCRYPT_N = 2 ** 17;
const SCRYPT_R = 8;
const SCRYPT_P = 1;
const SCRYPT_KEYLEN = 64;
// Node 默认 maxmem=32MB 装不下 N=2^17 的内存需求（128*N*r=128MB），需显式放宽
const SCRYPT_MAXMEM = 256 * 1024 * 1024;
// 验证时对存储参数设上限：防库里被塞入天文数字参数把线程池卡死
const SCRYPT_N_MAX = 2 ** 20;
const SCRYPT_R_MAX = 32;
const SCRYPT_P_MAX = 8;

function scryptAsync(password: string, salt: string, N: number, r: number, p: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    crypto.scrypt(password, salt, SCRYPT_KEYLEN, { N, r, p, maxmem: SCRYPT_MAXMEM }, (err, key) => {
      if (err) reject(err);
      else resolve(key);
    });
  });
}

// 异步版本跑在 libuv 线程池：登录峰值不再阻塞事件循环（旧 scryptSync 每次 ~50ms）
export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = await scryptAsync(password, salt, SCRYPT_N, SCRYPT_R, SCRYPT_P);
  return `scrypt2:${SCRYPT_N}:${SCRYPT_R}:${SCRYPT_P}:${salt}:${hash.toString("hex")}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const parts = stored.split(":");
  let salt: string, expectedHex: string, N: number, r: number, p: number;
  if (parts.length === 6 && parts[0] === "scrypt2") {
    N = Number(parts[1]); r = Number(parts[2]); p = Number(parts[3]);
    if (!Number.isInteger(N) || !Number.isInteger(r) || !Number.isInteger(p)) return false;
    if (N < 2 || r < 1 || p < 1 || N > SCRYPT_N_MAX || r > SCRYPT_R_MAX || p > SCRYPT_P_MAX) return false;
    salt = parts[4]; expectedHex = parts[5];
  } else if (parts.length === 3 && parts[0] === "scrypt") {
    N = 16384; r = 8; p = 1; // 旧格式（Node 默认参数），登录成功后会 rehash 升级
    salt = parts[1]; expectedHex = parts[2];
  } else {
    return false;
  }
  const expected = Buffer.from(expectedHex, "hex");
  if (expected.length !== SCRYPT_KEYLEN) return false;
  try {
    const candidate = await scryptAsync(password, salt, N, r, p);
    return crypto.timingSafeEqual(candidate, expected);
  } catch {
    return false;
  }
}

// 旧格式 / 弱参数条目：验证通过后应尽快用当前参数重哈希
export function passwordNeedsRehash(stored: string): boolean {
  const parts = stored.split(":");
  if (parts.length === 6 && parts[0] === "scrypt2") {
    return Number(parts[1]) < SCRYPT_N || Number(parts[2]) !== SCRYPT_R || Number(parts[3]) !== SCRYPT_P;
  }
  return true;
}

export interface JwtClaims {
  sub: string;
  name: string;
  iat: number;
  exp: number;
}

export function signJwt(claims: { sub: string; name: string }, secret: string, ttlMs: number): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const now = Date.now();
  const body = Buffer.from(JSON.stringify({
    sub: claims.sub,
    name: claims.name,
    iat: Math.floor(now / 1000),
    exp: Math.floor((now + ttlMs) / 1000),
  })).toString("base64url");
  const sig = crypto.createHmac("sha256", secret).update(`${header}.${body}`).digest("base64url");
  return `${header}.${body}.${sig}`;
}

export function verifyJwt(token: string, secret: string): JwtClaims | undefined {
  const parts = token.split(".");
  if (parts.length !== 3) return undefined;
  const expected = crypto.createHmac("sha256", secret).update(`${parts[0]}.${parts[1]}`).digest();
  const got = Buffer.from(parts[2], "base64url");
  if (got.length !== expected.length || !crypto.timingSafeEqual(got, expected)) return undefined;
  try {
    const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString()) as JwtClaims;
    if (typeof claims.sub !== "string" || typeof claims.exp !== "number") return undefined;
    if (claims.exp * 1000 <= Date.now()) return undefined;
    return claims;
  } catch {
    return undefined;
  }
}
