export type LogLevel = "debug" | "info" | "warn" | "error";

const LEVELS: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

let currentLevel = LEVELS.info;

export function setLogLevel(level: string): void {
  const normalized = level.toLowerCase() === "warning" ? "warn" : level.toLowerCase();
  if (normalized in LEVELS) {
    currentLevel = LEVELS[normalized as LogLevel];
  }
}

function log(level: LogLevel, msg: string, fields?: Record<string, unknown>): void {
  if (LEVELS[level] < currentLevel) return;
  const extra = fields
    ? " " + Object.entries(fields).map(([k, v]) => `${k}=${String(v)}`).join(" ")
    : "";
  const out = `${new Date().toISOString()} ${level.toUpperCase()} ${msg}${extra}`;
  if (level === "error" || level === "warn") {
    console.error(out);
  } else {
    console.log(out);
  }
}

export const logger = {
  debug: (msg: string, fields?: Record<string, unknown>) => log("debug", msg, fields),
  info: (msg: string, fields?: Record<string, unknown>) => log("info", msg, fields),
  warn: (msg: string, fields?: Record<string, unknown>) => log("warn", msg, fields),
  error: (msg: string, fields?: Record<string, unknown>) => log("error", msg, fields),
};

export function envString(key: string, def: string): string {
  const v = process.env[key];
  return v !== undefined && v !== "" ? v : def;
}

// Parses Go-style durations like "90s", "5m", "1h30m" into milliseconds.
export function parseDurationMs(v: string): number | undefined {
  const re = /(\d+(?:\.\d+)?)(ns|us|ms|s|m|h)/g;
  let total = 0;
  let matched = "";
  let m: RegExpExecArray | null;
  while ((m = re.exec(v)) !== null) {
    matched += m[0];
    const n = parseFloat(m[1]);
    switch (m[2]) {
      case "ns": total += n / 1e6; break;
      case "us": total += n / 1e3; break;
      case "ms": total += n; break;
      case "s": total += n * 1000; break;
      case "m": total += n * 60_000; break;
      case "h": total += n * 3_600_000; break;
    }
  }
  if (matched !== v || matched === "") return undefined;
  return total;
}

export function envDurationMs(key: string, defMs: number): number {
  const v = process.env[key];
  if (v === undefined || v === "") return defMs;
  const parsed = parseDurationMs(v);
  return parsed !== undefined ? parsed : defMs;
}

export interface FlagSpec {
  name: string;
  type: "string" | "duration";
  default: string;
}

// Parses CLI flags Go-style: "-name value", "-name=value", "--name value",
// "--name=value". Unknown flags are ignored.
export function parseFlags(specs: FlagSpec[], args: string[] = process.argv.slice(2)): Record<string, string> {
  const out: Record<string, string> = {};
  for (const s of specs) out[s.name] = s.default;
  const known = new Set(specs.map((s) => s.name));

  for (let i = 0; i < args.length; i++) {
    let arg = args[i];
    if (!arg.startsWith("-") || arg === "-") continue;
    arg = arg.replace(/^--?/, "");
    let name = arg;
    let value: string | undefined;
    const eq = arg.indexOf("=");
    if (eq !== -1) {
      name = arg.slice(0, eq);
      value = arg.slice(eq + 1);
    }
    if (!known.has(name)) continue;
    if (value === undefined) {
      if (i + 1 >= args.length) break;
      value = args[++i];
    }
    out[name] = value;
  }
  return out;
}

export function parseListenAddr(addr: string): { host?: string; port: number } {
  if (addr.startsWith(":")) return { port: Number(addr.slice(1)) };
  const idx = addr.lastIndexOf(":");
  if (idx === -1) return { port: Number(addr) };
  return { host: addr.slice(0, idx), port: Number(addr.slice(idx + 1)) };
}
