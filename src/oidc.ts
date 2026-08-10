// OIDC 授权码 + PKCE 登录骨架，全部基于 node:crypto 与全局 fetch，无外部依赖。
// - discovery / JWKS 懒加载并缓存（1h），失败时下次重试
// - state 为无状态 HMAC 签名令牌（含 nonce、PKCE verifier、过期时间），
//   签名密钥派生自网关 JWT 密钥，多实例/重启均可验证
// - id_token 验签支持 RS256 / ES256，校验 iss/aud/exp/nonce

import crypto from "node:crypto";

export interface OIDCConfig {
  issuer: string;
  clientID: string;
  clientSecret: string;
  redirectURL: string;
  employeeClaim: string; // 工号所在 claim，默认 "employee_id"
}

export interface OIDCIdentity {
  employeeID: string;
  displayName: string;
}

interface DiscoveryDoc {
  authorization_endpoint: string;
  token_endpoint: string;
  jwks_uri: string;
}

type Jwk = crypto.JsonWebKey & { kid?: string };

interface TokenResponse {
  id_token?: string;
  access_token?: string;
  error?: string;
  error_description?: string;
}

const CACHE_TTL_MS = 3_600_000;
const STATE_TTL_MS = 600_000;

export class OIDCProvider {
  private cfg: OIDCConfig;
  private stateSecret: Buffer;
  private discoveryDoc?: { doc: DiscoveryDoc; fetchedAt: number };
  private jwksCache?: { keys: Jwk[]; fetchedAt: number };

  constructor(cfg: OIDCConfig, jwtSecret: string) {
    this.cfg = cfg;
    // state 签名密钥从网关 JWT 密钥派生：多实例共享、重启不失效
    this.stateSecret = crypto.createHmac("sha256", jwtSecret).update("oidc-state").digest();
  }

  private issuerNoSlash(): string {
    return this.cfg.issuer.replace(/\/+$/, "");
  }

  private async discovery(): Promise<DiscoveryDoc> {
    if (this.discoveryDoc && Date.now() - this.discoveryDoc.fetchedAt < CACHE_TTL_MS) {
      return this.discoveryDoc.doc;
    }
    const resp = await fetch(`${this.issuerNoSlash()}/.well-known/openid-configuration`);
    if (!resp.ok) throw new Error(`discovery failed: ${resp.status}`);
    const doc = (await resp.json()) as DiscoveryDoc;
    if (!doc.authorization_endpoint || !doc.token_endpoint || !doc.jwks_uri) {
      throw new Error("discovery document incomplete");
    }
    this.discoveryDoc = { doc, fetchedAt: Date.now() };
    return doc;
  }

  private signState(payload: string): string {
    return crypto.createHmac("sha256", this.stateSecret).update(payload).digest("base64url");
  }

  // 生成授权跳转 URL；state 内含 nonce 与 PKCE verifier，回调时自校验
  async buildAuthURL(): Promise<string> {
    const doc = await this.discovery();
    const nonce = crypto.randomBytes(16).toString("base64url");
    const verifier = crypto.randomBytes(32).toString("base64url");
    const payload = Buffer.from(JSON.stringify({
      n: nonce,
      v: verifier,
      e: Date.now() + STATE_TTL_MS,
    })).toString("base64url");
    const state = `${payload}.${this.signState(payload)}`;
    const challenge = crypto.createHash("sha256").update(verifier).digest("base64url");
    const url = new URL(doc.authorization_endpoint);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("client_id", this.cfg.clientID);
    url.searchParams.set("redirect_uri", this.cfg.redirectURL);
    url.searchParams.set("scope", "openid profile");
    url.searchParams.set("state", state);
    url.searchParams.set("nonce", nonce);
    url.searchParams.set("code_challenge", challenge);
    url.searchParams.set("code_challenge_method", "S256");
    return url.toString();
  }

  private verifyState(state: string): { nonce: string; verifier: string } | undefined {
    const dot = state.lastIndexOf(".");
    if (dot <= 0) return undefined;
    const payload = state.slice(0, dot);
    const sig = Buffer.from(state.slice(dot + 1), "base64url");
    const expected = Buffer.from(this.signState(payload), "base64url");
    if (sig.length !== expected.length || !crypto.timingSafeEqual(sig, expected)) return undefined;
    try {
      const s = JSON.parse(Buffer.from(payload, "base64url").toString()) as { n: string; v: string; e: number };
      if (typeof s.n !== "string" || typeof s.v !== "string" || typeof s.e !== "number") return undefined;
      if (s.e <= Date.now()) return undefined;
      return { nonce: s.n, verifier: s.v };
    } catch {
      return undefined;
    }
  }

  private async fetchJwks(): Promise<Jwk[]> {
    if (this.jwksCache && Date.now() - this.jwksCache.fetchedAt < CACHE_TTL_MS) {
      return this.jwksCache.keys;
    }
    const doc = await this.discovery();
    const resp = await fetch(doc.jwks_uri);
    if (!resp.ok) throw new Error(`jwks fetch failed: ${resp.status}`);
    const body = (await resp.json()) as { keys?: Jwk[] };
    const keys = body.keys ?? [];
    this.jwksCache = { keys, fetchedAt: Date.now() };
    return keys;
  }

  private async exchangeCode(code: string, verifier: string): Promise<TokenResponse> {
    const doc = await this.discovery();
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: this.cfg.redirectURL,
      code_verifier: verifier,
    });
    const resp = await fetch(doc.token_endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: "Basic " + Buffer.from(`${this.cfg.clientID}:${this.cfg.clientSecret}`).toString("base64"),
      },
      body,
    });
    if (!resp.ok) throw new Error(`token exchange failed: ${resp.status}`);
    return (await resp.json()) as TokenResponse;
  }

  private async verifyIdToken(idToken: string, nonce: string): Promise<Record<string, unknown>> {
    const parts = idToken.split(".");
    if (parts.length !== 3) throw new Error("malformed id_token");
    const header = JSON.parse(Buffer.from(parts[0], "base64url").toString()) as { alg?: string; kid?: string };
    const alg = header.alg === "ES256" ? "sha256" : header.alg === "RS256" ? "RSA-SHA256" : undefined;
    if (!alg) throw new Error(`unsupported id_token alg: ${header.alg}`);

    let keys = await this.fetchJwks();
    let jwk = keys.find((k) => k.kid === header.kid);
    if (!jwk) {
      // kid 未命中可能是密钥轮换，强制刷新一次
      this.jwksCache = undefined;
      keys = await this.fetchJwks();
      jwk = keys.find((k) => k.kid === header.kid);
      if (!jwk) throw new Error("no matching jwk");
    }
    const key = crypto.createPublicKey({ key: jwk, format: "jwk" });
    const data = Buffer.from(`${parts[0]}.${parts[1]}`);
    const sig = Buffer.from(parts[2], "base64url");
    const ok = header.alg === "ES256"
      ? crypto.verify(alg, data, { key, dsaEncoding: "ieee-p1363" }, sig)
      : crypto.verify(alg, data, key, sig);
    if (!ok) throw new Error("id_token signature invalid");

    const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString()) as Record<string, unknown>;
    if (claims.iss !== this.cfg.issuer && claims.iss !== this.issuerNoSlash()) {
      throw new Error("issuer mismatch");
    }
    const aud = claims.aud;
    const audOk = typeof aud === "string" ? aud === this.cfg.clientID
      : Array.isArray(aud) && aud.includes(this.cfg.clientID);
    if (!audOk) throw new Error("audience mismatch");
    if (typeof claims.exp !== "number" || claims.exp * 1000 <= Date.now()) {
      throw new Error("id_token expired");
    }
    if (claims.nonce !== nonce) throw new Error("nonce mismatch");
    return claims;
  }

  // 回调全链路：校验 state → 换 code → 验 id_token → 提取身份
  async authenticate(code: string, state: string): Promise<OIDCIdentity> {
    const s = this.verifyState(state);
    if (!s) throw new Error("invalid or expired state");
    const tokens = await this.exchangeCode(code, s.verifier);
    if (!tokens.id_token) throw new Error(tokens.error_description ?? tokens.error ?? "no id_token");
    const claims = await this.verifyIdToken(tokens.id_token, s.nonce);

    const employeeID = claims[this.cfg.employeeClaim] ?? claims.sub;
    if (typeof employeeID !== "string" || employeeID === "") {
      throw new Error(`missing employee claim: ${this.cfg.employeeClaim}`);
    }
    const name = claims.name ?? claims.preferred_username;
    return {
      employeeID,
      displayName: typeof name === "string" && name !== "" ? name : employeeID,
    };
  }
}
