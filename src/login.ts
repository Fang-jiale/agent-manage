// 命令行登录工具：调用 gateway 的 /auth/login 获取 JWT。
// 用法：node src/login.ts -name admin -password admin123
// 输出仅为 token，便于：TOKEN=$(node src/login.ts ...)

import { envString, parseFlags } from "./util.ts";

const specs = [
  { name: "gateway", type: "string" as const, default: envString("AGENT_MANAGE_GATEWAY_HTTP", "http://localhost:8080") },
  { name: "name", type: "string" as const, default: envString("AGENT_MANAGE_USER", "admin") },
  { name: "password", type: "string" as const, default: envString("AGENT_MANAGE_PASSWORD", "") },
];

const values = parseFlags(specs);
if (values["password"] === "") {
  console.error("缺少密码：-password <pwd> 或 AGENT_MANAGE_PASSWORD");
  process.exit(1);
}

const res = await fetch(`${values["gateway"]}/auth/login`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ name: values["name"], password: values["password"] }),
});

if (!res.ok) {
  console.error(`登录失败：HTTP ${res.status} ${await res.text()}`);
  process.exit(1);
}

const data = (await res.json()) as { token: string };
console.log(data.token);
