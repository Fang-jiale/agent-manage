// 内存滑动窗口限流器（单实例维度；多实例全局限额需换 Redis 计数器，当前规模不需要）。

export class RateLimiter {
  private hits = new Map<string, number[]>();
  private limit: number;
  private windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.limit = limit;
    this.windowMs = windowMs;
    const sweeper = setInterval(() => this.sweep(), windowMs);
    sweeper.unref();
  }

  // 允许则记录并返回 true，否则 false
  allow(key: string): boolean {
    const now = Date.now();
    const arr = (this.hits.get(key) ?? []).filter((t) => now - t < this.windowMs);
    if (arr.length >= this.limit) {
      this.hits.set(key, arr);
      return false;
    }
    arr.push(now);
    this.hits.set(key, arr);
    return true;
  }

  private sweep(): void {
    const now = Date.now();
    for (const [key, arr] of this.hits) {
      const kept = arr.filter((t) => now - t < this.windowMs);
      if (kept.length === 0) this.hits.delete(key);
      else this.hits.set(key, kept);
    }
  }
}

// 取客户端 IP：仅在 trustProxy（前面有可信反代）时采用 X-Forwarded-For 首段，
// 否则用 socket 地址——直接暴露时 XFF 可被客户端伪造，会绕过按 IP 的限流
export function clientIp(headers: Record<string, string | string[] | undefined>, fallback: string | undefined, trustProxy = false): string {
  if (trustProxy) {
    const xff = headers["x-forwarded-for"];
    const first = Array.isArray(xff) ? xff[0] : xff;
    if (first) return first.split(",")[0].trim();
  }
  return fallback ?? "unknown";
}
