// JWT (HS256) 签发/校验与 scrypt 密码哈希，全部基于 node:crypto，无外部依赖。

import crypto from "node:crypto";

export function hashPassword(password: string): string {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return `scrypt:${salt}:${hash}`;
}

export function verifyPassword(password: string, stored: string): boolean {
  const parts = stored.split(":");
  if (parts.length !== 3 || parts[0] !== "scrypt") return false;
  const candidate = crypto.scryptSync(password, parts[1], 64);
  const expected = Buffer.from(parts[2], "hex");
  return candidate.length === expected.length && crypto.timingSafeEqual(candidate, expected);
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
