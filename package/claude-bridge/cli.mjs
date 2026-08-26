// 桥接器:shim 在自己目录找 cli.mjs 并以 `node cli.mjs <args>` 方式拉起 CLI;
// 这里把同样的参数原样转给真正的 Claude Code,stdio 直通
import { spawn } from "node:child_process";
const child = spawn("claude", process.argv.slice(2), { stdio: "inherit" });
child.on("error", (e) => { console.error("[claude-bridge] 启动 claude 失败:", e.message); process.exit(1); });
child.on("exit", (code, sig) => process.exit(code ?? (sig ? 128 : 1)));
