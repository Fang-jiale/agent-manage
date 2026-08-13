#!/usr/bin/env node
// 离线包组装：dist/bundle/*.mjs + static/ + 预置 Node 运行时 → 4 个平台包。
// 用法：npm run bundle && node scripts/build-offline.mjs
// 运行时二进制放在 dist/offline/runtime/{linux-x64,linux-arm64,win-x64}/（不重新下载）。
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BUNDLE = path.join(ROOT, "dist", "bundle");
const STATIC = path.join(ROOT, "static");
const RUNTIME = path.join(ROOT, "dist", "offline", "runtime");
const OUT = path.join(ROOT, "dist", "offline", "packages");

const GATEWAY_SERVICE = `[Unit]
Description=YwMatrix Gateway
After=network-online.target mysql.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/opt/ywmatrix/runtime/node /opt/ywmatrix/bin/gateway.mjs -addr 0.0.0.0:8080
Restart=always
RestartSec=5
# 网关在 SIGTERM 后进入 drain：healthz 立即 503，断 WS，约 1.5s 后退出
TimeoutStopSec=15
User=ywmatrix
Group=ywmatrix
WorkingDirectory=/opt/ywmatrix
Environment=AGENT_MANAGE_DATABASE_URL=mysql://ywmatrix:CHANGE_ME@127.0.0.1:3306/ywmatrix
# JWT secret：默认从 /opt/ywmatrix/data/jwt-secret 自动生成并持久化（推荐）；
# 如要强制指定或与其它实例共享，再取消下一行注释（至少 32 字节随机串）：
# Environment=AGENT_MANAGE_JWT_SECRET=
Environment=AGENT_MANAGE_ADMIN_PASSWORD=CHANGE_ME
# 多实例时启用（需另装 Redis）：
# Environment=AGENT_MANAGE_REDIS_URL=redis://127.0.0.1:6379
# Environment=AGENT_MANAGE_INSTANCE_ID=gw-1

[Install]
WantedBy=multi-user.target
`;

const SERVER_INSTALL = `# YwMatrix 网关离线安装包（Linux x86_64，自带 Node 18 运行时）

## 内容

\`\`\`
bin/gateway.mjs   网关单文件（全部依赖已打入，Node >= 18 即可运行）
bin/login.mjs     命令行登录取 JWT 的小工具
bin/client.mjs    AgentClient（网关机上也要跑 agent 时用）
bin/local-agent.mjs  示例本地 Agent（demo/联调用）
static/           管理页面（网关按相对路径 ../static 引用，勿移动）
runtime/node      Node.js 18 linux-x64 运行时（系统已有 Node >= 18 可不用）
ywmatrix-gateway.service   systemd 服务模板
\`\`\`

## 安装步骤

\`\`\`bash
# 1. 解压到 /opt
sudo mkdir -p /opt/ywmatrix
sudo tar xzf ywmatrix-server-linux-x64.tar.gz -C /opt/ywmatrix --strip-components=1

# 2. 建运行账号和附件目录
sudo useradd -r -s /sbin/nologin ywmatrix
sudo mkdir -p /opt/ywmatrix/data/attachments
sudo chown -R ywmatrix:ywmatrix /opt/ywmatrix

# 3. 准备 MySQL（8.0+；内网已有则只执行下面 SQL）
#    CREATE DATABASE IF NOT EXISTS ywmatrix CHARACTER SET utf8mb4;
#    CREATE USER IF NOT EXISTS 'ywmatrix'@'%' IDENTIFIED BY '强密码';
#    GRANT ALL PRIVILEGES ON ywmatrix.* TO 'ywmatrix'@'%';

# 4. 配置 systemd（替换两处 CHANGE_ME：数据库密码 / admin 初始密码）
sudo cp /opt/ywmatrix/ywmatrix-gateway.service /etc/systemd/system/
sudo vi /etc/systemd/system/ywmatrix-gateway.service
sudo systemctl daemon-reload
sudo systemctl enable --now ywmatrix-gateway
curl http://127.0.0.1:8080/healthz     # {"status":"ok",...} 即就绪
\`\`\`

浏览器访问 \`http://服务器IP:8080\`，admin + 上面设置的初始密码登录。

## 数据库：不需要手工执行迁移 SQL

网关启动时自动完成全部建表与迁移（幂等，可反复执行）：

- 首启：创建 users / sessions / messages / agents / device_keys / agent_brands / pairing_codes 等全部表；
- 老库升级：自动补 agents.brand_id/connector_id/approval_status、agent_brands.launch_cmd/conn_type/endpoint 等新增列（ALTER 已存在则跳过）。

升级只需：停服 → 覆盖 bin/ 与 static/ → 启服。

## 品牌治理模式（重要行为变化）

- **不建品牌 = 开放模式**：agent 自由注册，行为与旧版完全一致。
- **一旦在「品牌管理」建了品牌**：注册必须带合法 brand_id，老版本/自由注册的 client 会被拒绝。
  推荐全部改用 connector 模式接入（配对码 → 页面分配实例），见客户端 README。

## 说明

- 系统若已装 Node（>= 18），可把 service 里 ExecStart 的 runtime/node 改为 /usr/bin/node。
- 防火墙放行终端到服务器 8080 入站；单机部署无需 Redis。
- 生产建议加 Nginx/Caddy 反代终结 TLS：放行 WebSocket Upgrade 头、client_max_body_size 32m、proxy_read_timeout 3600s。
- 运维端点：GET /healthz（就绪探针）、GET /metrics（Prometheus 指标）。
`;

const CLIENT_START_SH = `#!/usr/bin/env bash
# YwMatrix AgentClient —— 首次接入：填 2 个 CHANGE_ME 后 ./start.sh（需管理员在后台「待接入」批准）
# 批准后凭证自动写入 ~/.agent-manage/connector.json，之后 ./start.sh 零参数即可
set -euo pipefail
cd "$(dirname "$0")"

GATEWAY=ws://CHANGE_ME_SERVER:8080/ws/agent
PAIR_CODE=CHANGE_ME_PAIR_CODE

CFG="$HOME/.agent-manage/connector.json"
if [ -f "$CFG" ]; then
  exec ./runtime/node ./client.mjs
fi
if [ -z "$PAIR_CODE" ] || [ "$PAIR_CODE" = "CHANGE_ME_PAIR_CODE" ]; then
  echo "尚未接入：请填 start.sh 里的 GATEWAY 和 PAIR_CODE（配对码在管理后台「设备密钥」页生成）"
  echo "或者直接浏览器打开本机 http://127.0.0.1:9321 在页面上完成配对"
  exec ./runtime/node ./client.mjs
fi
exec ./runtime/node ./client.mjs -gateway "$GATEWAY" -pair "$PAIR_CODE"
`;

const CLIENT_START_BAT = `@echo off
REM ============================================================
REM  YwMatrix AgentClient（Windows）
REM  首次接入：改下面 2 个 CHANGE_ME 后双击运行（需管理员在后台批准）
REM  之后凭证已写入 %USERPROFILE%\\.agent-manage\\connector.json，零参数
REM ============================================================
set GATEWAY=ws://CHANGE_ME_SERVER:8080/ws/agent
set PAIR_CODE=CHANGE_ME_PAIR_CODE

if exist "%USERPROFILE%\\.agent-manage\\connector.json" (
  "%~dp0runtime\\node.exe" "%~dp0client.mjs"
) else (
  "%~dp0runtime\\node.exe" "%~dp0client.mjs" -gateway %GATEWAY% -pair %PAIR_CODE%
)
pause
`;

const CLIENT_SERVICE = `[Unit]
Description=YwMatrix AgentClient
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# 注意：先用 ./start.sh 手工完成一次配对接入（凭证写入运行账号的
# ~/.agent-manage/connector.json），再 enable 本服务；User 必须是配对时用的账号
ExecStart=/opt/ywmatrix-client/runtime/node /opt/ywmatrix-client/client.mjs
Restart=always
RestartSec=5
User=CHANGE_ME_USER
WorkingDirectory=/opt/ywmatrix-client

[Install]
WantedBy=multi-user.target
`;

const CLIENT_README_COMMON = `
## 日常

- 启动：运行启动脚本即可（零参数，读已落盘的配置）
- 管理本机 agent：浏览器打开 \`http://127.0.0.1:9321\` —— 查看/添加/移除实例、
  改启动命令或服务地址。agent 类型（品牌）由管理员在管控台维护，本地只能从目录里选
- 连接方式三种：stdio（命令拉起子进程）/ http / ws（服务地址），由品牌定义；
  本地可对单个实例做覆盖（比如本机路径不同）
`;

function clientReadmeLinux(archNote) {
  return `# YwMatrix AgentClient 终端包（Linux ${archNote}，自带 Node 18 运行时，免安装）

解压即用，无需装 Node。

## 首次接入（配对）

1. 管理员在管理后台 \`http://网关地址/admin\` →「设备密钥」→「生成配对码」，把码发给你
2. 编辑 \`start.sh\` 填入 \`GATEWAY\`（网关地址）和 \`PAIR_CODE\`（配对码）
3. \`chmod +x start.sh runtime/node && ./start.sh\`
4. 通知管理员在后台「Agent 管理 → 待接入」点**批准**；批准后自动接入完成，
   凭证写入 \`~/.agent-manage/connector.json\`

> 也可以跳过编辑脚本：直接 \`./runtime/node ./client.mjs\`，然后浏览器打开
> 本机 \`http://127.0.0.1:9321\`，在页面上填网关地址和配对码完成接入。
${CLIENT_README_COMMON}
## 开机自启（systemd）

\`\`\`bash
# 先按上面完成一次配对接入（用将来运行服务的同一个账号！）
sudo mkdir -p /opt/ywmatrix-client
sudo cp -r ./* /opt/ywmatrix-client/
sudo cp ywmatrix-client.service /etc/systemd/system/
sudo vi /etc/systemd/system/ywmatrix-client.service   # 替换 User=CHANGE_ME_USER
sudo systemctl daemon-reload && sudo systemctl enable --now ywmatrix-client
\`\`\`

## 排错

- 连不上网关：\`curl http://网关:8080/healthz\`，检查防火墙出站
- 配对报错 invalid or expired：配对码一次性且默认 24h 有效，让管理员重新生成
- 要换网关/重新接入：删除 \`~/.agent-manage/connector.json\` 后重新走配对
- 页面看不到 agent：让管理员确认管控台「Agent 管理」里已给本 connector 分配实例
`;
}

const CLIENT_README_WIN = `# YwMatrix AgentClient 终端包（Windows x64，自带 Node 18 运行时，免安装）

解压即用，无需装 Node。

## 首次接入（配对）

1. 管理员在管理后台 \`http://网关地址/admin\` →「设备密钥」→「生成配对码」，把码发给你
2. 记事本打开 \`start.bat\`，填入 \`GATEWAY\`（网关地址）和 \`PAIR_CODE\`（配对码），保存
3. 双击 \`start.bat\`
4. 通知管理员在后台「Agent 管理 → 待接入」点**批准**；批准后自动接入完成，
   凭证写入 \`%USERPROFILE%\\.agent-manage\\connector.json\`

> 也可以跳过编辑脚本：命令行运行 \`runtime\\node.exe client.mjs\`，然后浏览器打开
> 本机 \`http://127.0.0.1:9321\`，在页面上填网关地址和配对码完成接入。
${CLIENT_README_COMMON}
## 开机自启

任务计划程序 → 创建任务 → 触发器"登录时"→ 操作指向 \`start.bat\`
（先用同一 Windows 账号完成一次配对接入再建任务）。

## 排错

- 连不上网关：浏览器访问 \`http://网关:8080/healthz\`，检查防火墙出站
- 配对报错 invalid or expired：配对码一次性且默认 24h 有效，让管理员重新生成
- 要换网关/重新接入：删除 \`%USERPROFILE%\\.agent-manage\\connector.json\` 后重新走配对
- 页面看不到 agent：让管理员确认管控台「Agent 管理」里已给本 connector 分配实例
`;

function cp(src, dst) {
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  fs.copyFileSync(src, dst);
}

function write(file, content, mode) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, mode ? { mode } : undefined);
}

function assemble(name, files) {
  const dir = path.join(OUT, name);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  for (const f of files) f(dir);
  return dir;
}

function mustExist(p) {
  if (!fs.existsSync(p)) throw new Error(`缺少文件：${p}（先 npm run bundle）`);
}

for (const f of ["gateway.mjs", "client.mjs", "local-agent.mjs", "login.mjs"]) mustExist(path.join(BUNDLE, f));
for (const r of ["linux-x64/node", "linux-arm64/node", "win-x64/node.exe"]) mustExist(path.join(RUNTIME, r));

// ---- 服务端（Linux x64）----
const server = assemble("ywmatrix-server-linux-x64", [
  (d) => { for (const f of ["gateway.mjs", "client.mjs", "local-agent.mjs", "login.mjs"]) cp(path.join(BUNDLE, f), path.join(d, "bin", f)); },
  (d) => { for (const f of fs.readdirSync(STATIC)) cp(path.join(STATIC, f), path.join(d, "static", f)); },
  (d) => cp(path.join(RUNTIME, "linux-x64", "node"), path.join(d, "runtime", "node")),
  (d) => write(path.join(d, "ywmatrix-gateway.service"), GATEWAY_SERVICE),
  (d) => write(path.join(d, "INSTALL.md"), SERVER_INSTALL),
]);

// ---- 客户端（Linux x64 / arm64，Windows x64）----
function clientFiles(runtimeRel, readme, isWin) {
  return [
    (d) => cp(path.join(BUNDLE, "client.mjs"), path.join(d, "client.mjs")),
    (d) => cp(path.join(BUNDLE, "local-agent.mjs"), path.join(d, "local-agent.mjs")),
    (d) => { fs.mkdirSync(path.join(d, "static"), { recursive: true }); cp(path.join(STATIC, "client.html"), path.join(d, "static", "client.html")); },
    (d) => cp(path.join(RUNTIME, runtimeRel), path.join(d, "runtime", isWin ? "node.exe" : "node")),
    (d) => write(path.join(d, isWin ? "start.bat" : "start.sh"), isWin ? CLIENT_START_BAT : CLIENT_START_SH, isWin ? undefined : 0o755),
    (d) => write(path.join(d, "README.md"), readme),
    ...(isWin ? [] : [(d) => write(path.join(d, "ywmatrix-client.service"), CLIENT_SERVICE)]),
  ];
}
const clientLinuxX64 = assemble("ywmatrix-client-linux-x64", clientFiles("linux-x64/node", clientReadmeLinux("x86_64"), false));
const clientLinuxArm64 = assemble("ywmatrix-client-linux-arm64", clientFiles("linux-arm64/node", clientReadmeLinux("arm64（麒麟/统信、飞腾/鲲鹏）"), false));
const clientWinX64 = assemble("ywmatrix-client-win-x64", clientFiles("win-x64/node.exe", CLIENT_README_WIN, true));

// ---- 归档 ----
for (const dir of [server, clientLinuxX64, clientLinuxArm64]) {
  execFileSync("tar", ["-czf", `${dir}.tar.gz`, "-C", OUT, path.basename(dir)], { stdio: "inherit" });
}
execFileSync("zip", ["-qr", `${clientWinX64}.zip`, path.basename(clientWinX64)], { cwd: OUT, stdio: "inherit" });

for (const f of fs.readdirSync(OUT).filter((f) => f.endsWith(".tar.gz") || f.endsWith(".zip"))) {
  const st = fs.statSync(path.join(OUT, f));
  console.log(`${f}  ${(st.size / 1024 / 1024).toFixed(1)} MB`);
}
console.log("done →", OUT);
