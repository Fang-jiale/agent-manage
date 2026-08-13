# 部署指南

> 离线包（自带 Node 运行时的服务端/终端包）的部署与使用见 [离线包部署与使用手册](offline-guide.md)；打包方法：`npm run bundle && node scripts/build-offline.mjs`。本文档面向源码部署。

## 1. 部署前准备

### 1.1 确认目标平台

本项目为 Node.js (TypeScript) 实现，无需编译，目标机器安装 **Node.js >= 24** 即可运行。

| 平台 | 常见 CPU 架构 | 说明 |
|------|--------------|------|
| Windows 10/11 | x64 / arm64 | 安装 Node.js 24+ 即可 |
| Linux x64 | x64 | 安装 Node.js 24+ 即可 |
| 麒麟桌面（飞腾/鲲鹏） | arm64 | 使用官方 linux-arm64 发行版 |
| 麒麟桌面（x86） | x64 | 使用官方 linux-x64 发行版 |
| macOS Intel | x64 | 安装 Node.js 24+ 即可 |
| macOS Apple Silicon | arm64 | 安装 Node.js 24+ 即可 |

> 龙芯（loong64）等非官方架构需自行编译 Node.js；Node.js 官方不支持 Windows 7/8。

### 1.2 网络要求

- 终端必须能访问网关的内网地址和端口
- 网关不需要访问终端，只需要监听端口
- 所有通信建议走 HTTPS/WSS（生产环境）

## 2. 打包分发

### 2.1 生成 tarball

```bash
npm pack
```

生成 `agent-manage-1.0.0.tgz`，拷贝到目标机器后：

```bash
tar xzf agent-manage-1.0.0.tgz
cd package
npm install --omit=dev
node src/gateway.ts
```

也可以直接 `git clone` 或 `scp` 整个源码目录。

### 2.2 单文件可执行程序（可选）

如需免安装 Node 的运行方式，可使用 Node 官方的 SEA（Single Executable Applications）机制将脚本和 Node 运行时合并为单个可执行文件，本项目暂未内置该流程，详见 [Node.js SEA 文档](https://nodejs.org/api/single-executable-applications.html)。

## 3. 网关部署

### 3.0 使用 Docker Compose（最快）

仓库 `deploy/docker-compose.yml` 一把起 MySQL 8 + Redis 7 + 网关：

```bash
cd deploy
echo "AGENT_MANAGE_JWT_SECRET=$(openssl rand -hex 32)" > .env
docker compose up -d
# 打开 http://localhost:8080 ，admin / admin123（.env 中可改 AGENT_MANAGE_ADMIN_PASSWORD）

# 附件走 MinIO 对象存储（可选）：
echo "AGENT_MANAGE_S3_ENDPOINT=http://minio:9000" >> .env
docker compose --profile s3 up -d
```

网关镜像由仓库根目录 `Dockerfile` 构建（Node 24 直接运行 TS，无编译步骤）。

### 3.1 Linux 服务器部署

```bash
# 上传源码到服务器
scp -r ./agent-manage user@gateway-server:/opt/

cd /opt/agent-manage
npm install --omit=dev

# 启动
node src/gateway.ts -addr 0.0.0.0:8080
```

### 3.2 使用 systemd 管理（推荐）

可直接使用仓库 `deploy/agent-gateway.service`（含 MySQL/Redis/JWT 环境变量占位，替换 `CHANGE_ME` 后拷贝到 `/etc/systemd/system/`），或按下述模板手动创建 `/etc/systemd/system/agent-gateway.service`：

```ini
[Unit]
Description=Agent Manage Gateway
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/node /opt/agent-manage/src/gateway.ts -addr 0.0.0.0:8080
Restart=always
RestartSec=5
User=agent
Group=agent
WorkingDirectory=/opt/agent-manage

[Install]
WantedBy=multi-user.target
```

启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable agent-gateway
sudo systemctl start agent-gateway
```

### 3.3 使用 Nginx/Caddy 反向代理（生产推荐）

生产环境由反代终结 TLS，网关本身保持明文并只监听回环地址（`-addr 127.0.0.1:8080`）。页面会自动按 `https:` 切换为 `wss://`，无需改动前端。

完整示例见仓库 `deploy/nginx.conf` 与 `deploy/Caddyfile`，要点：

- **WebSocket 必须放行 Upgrade 头**（Caddy 开箱即用；Nginx 需 `proxy_set_header Upgrade/Connection`，示例用 `map $http_upgrade` 让非 WS 请求走 keepalive）
- **`client_max_body_size 32m`**：附件上传为 JSON+base64，20MB 文件约 28MB 请求体，Nginx 默认 1m 会直接 413
- **`proxy_read_timeout 3600s`**：WS 长连接；网关每 30s 有 ping 保活，此值防代理提前断连
- **`/files/` 可长缓存**：附件 URL 含 UUID 且响应带 immutable，缓存安全

多实例水平扩展：网关无状态（Redis 总线负责注册表与跨实例路由），反代按 round robin 分发即可，无需会话保持：

```nginx
upstream ywmatrix_gateway {
    server 127.0.0.1:8080;
    server 127.0.0.1:8081;
    keepalive 32;
}
```

每个实例设置相同的 `AGENT_MANAGE_JWT_SECRET`、`AGENT_MANAGE_DATABASE_URL`、`AGENT_MANAGE_REDIS_URL`，以及不同的 `AGENT_MANAGE_INSTANCE_ID`（缺省随机生成亦可）。

网关在可信反代之后时设置 `AGENT_MANAGE_TRUST_PROXY=true`，客户端 IP 才取 `X-Forwarded-For`（登录限流、agent `last_ip` 依赖它）；网关直接暴露时保持默认 `false`，否则限流可被伪造的 XFF 头绕过。

最小可用 Nginx 片段（仅 WS + TLS）：

```nginx
server {
    listen 443 ssl;
    server_name gateway.internal;
    ssl_certificate     /etc/nginx/ssl/gateway.crt;
    ssl_certificate_key /etc/nginx/ssl/gateway.key;
    client_max_body_size 32m;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 3600s;
    }
}
```

**注意**：WebSocket 必须配置 `Upgrade` 和 `Connection` 头。

## 4. 终端 Client 部署

> **认证方式**：下文示例用 `-token`（用户 JWT，会过期）。长期运行的终端推荐改用 `-key <设备密钥>`：管理员在 `http://网关/admin` 的「设备密钥」页创建（明文只显示一次），密钥不过期、可随时吊销，与 `-token` 二选一。

### 4.1 Windows 部署

#### 方式一：命令行启动

```powershell
# HTTP 型本地 Agent
node src\client.ts `
  -adapter http `
  -gateway ws://gateway.internal:8080/ws/agent `
  -agent-id PC-ZHANGSAN `
  -local-agent http://localhost:9001 `
  -token "$TOKEN"  # 通过 node src/login.ts 获取

# 命令行型本地 Agent（如 Claude Code 类 Agent）
node src\client.ts `
  -adapter stdio `
  -gateway ws://gateway.internal:8080/ws/agent `
  -agent-id PC-ZHANGSAN `
  -local-agent "C:\Program Files\MyAgent\agent.exe" `
  -token "$TOKEN"  # 通过 node src/login.ts 获取
```

#### 方式二：注册为 Windows 服务（推荐）

使用 `nssm`（Non-Sucking Service Manager）：

```powershell
nssm install AgentClient
# 在弹出的窗口中：
# Path: C:\Program Files\nodejs\node.exe
# Startup directory: C:\Program Files\AgentManage
# Arguments: src\client.ts -adapter http -gateway ws://gateway.internal:8080/ws/agent -agent-id PC-ZHANGSAN -local-agent http://localhost:9001 -token "$TOKEN"  # 通过 node src/login.ts 获取

nssm start AgentClient
```

### 4.2 Linux / 麒麟部署

```bash
# HTTP 型本地 Agent
node /opt/agent-manage/src/client.ts \
  -adapter http \
  -gateway ws://gateway.internal:8080/ws/agent \
  -agent-id kylin-zhangsan \
  -local-agent http://localhost:9001 \
  -token "$TOKEN"  # 通过 node src/login.ts 获取

# Stdio 型本地 Agent
node /opt/agent-manage/src/client.ts \
  -adapter stdio \
  -gateway ws://gateway.internal:8080/ws/agent \
  -agent-id kylin-zhangsan \
  -local-agent "/opt/my-agent/bin/agent" \
  -token "$TOKEN"  # 通过 node src/login.ts 获取
```

#### 使用 systemd

可直接使用仓库 `deploy/agent-client.service`（替换 `CHANGE_ME` 后拷贝到 `/etc/systemd/system/`），或手动创建 `/etc/systemd/system/agent-client.service`：

```ini
[Unit]
Description=Agent Client
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/node /opt/agent-manage/src/client.ts \
  -adapter http \
  -gateway ws://gateway.internal:8080/ws/agent \
  -agent-id kylin-zhangsan \
  -local-agent http://localhost:9001 \
  -token "$TOKEN"  # 通过 node src/login.ts 获取
Restart=always
RestartSec=5
User=zhangsan
Group=zhangsan
WorkingDirectory=/opt/agent-manage

[Install]
WantedBy=multi-user.target
```

启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable agent-client
sudo systemctl start agent-client
```

### 4.3 macOS 部署

```bash
# HTTP 型本地 Agent
node src/client.ts \
  -adapter http \
  -gateway ws://gateway.internal:8080/ws/agent \
  -agent-id mac-zhangsan \
  -local-agent http://localhost:9001 \
  -token "$TOKEN"  # 通过 node src/login.ts 获取

# Stdio 型本地 Agent
node src/client.ts \
  -adapter stdio \
  -gateway ws://gateway.internal:8080/ws/agent \
  -agent-id mac-zhangsan \
  -local-agent "/opt/my-agent/bin/agent" \
  -token "$TOKEN"  # 通过 node src/login.ts 获取
```

使用 `launchd` 后台运行：

创建 `~/Library/LaunchAgents/com.agent.client.plist`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.agent.client</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/node</string>
        <string>/opt/agent-manage/src/client.ts</string>
        <string>-adapter</string>
        <string>http</string>
        <string>-gateway</string>
        <string>ws://gateway.internal:8080/ws/agent</string>
        <string>-agent-id</string>
        <string>mac-zhangsan</string>
        <string>-local-agent</string>
        <string>http://localhost:9001</string>
        <string>-token</string>
        <string>通过 node src/login.ts 获取的 JWT</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
```

加载：

```bash
launchctl load ~/Library/LaunchAgents/com.agent.client.plist
```

## 5. 本地 Agent 部署

本地 Agent 需要与 AgentClient 同机运行，暴露 HTTP 服务。

### 5.1 使用 Demo 本地 Agent

```bash
node src/local-agent.ts -addr :9001
```

### 5.2 接入真实 Agent

只要真实 Agent 提供以下接口，AgentClient 就可以调用：

```http
GET /capabilities
Response:
{
  "capabilities": [
    {"type": "chat", "name": "coding", "description": "编码助手"}
  ]
}

POST /tasks
Request Body: {"task_id": "...", "session_id": "...", "type": "chat", "content": "..."}
Response: text/event-stream
```

SSE 响应格式（每个 `data:` 行是一条 JSON-RPC 通知，type 由 Agent 自行定义）：

```
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"...","type":"text","content":[{"type":"text","text":"收到指令"}],"done":false}}
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"...","type":"confirm_required","confirm_id":"c-1","content":[{"type":"text","text":"确认执行？"}]}}
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"...","type":"text","content":[{"type":"text","text":"完成"}],"done":true}}
```

## 6. 配置参数说明

### gateway

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-addr` | 监听地址（反代后端建议 `127.0.0.1:8080`） | `:8080` |
| `-log-level` | 日志级别 | `info` |
| `-agent-timeout` | Agent 心跳超时 | `90s` |
| `-user-timeout` | 用户心跳超时 | `120s` |
| `-task-timeout` | 任务超时 | `30m` |
| `-database-url` | MySQL 连接串 | `mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix` |
| `-jwt-secret` | JWT 签名密钥（**生产必填**） | 空（随机生成，重启后 token 全失效） |
| `-jwt-ttl` | JWT 有效期 | `7d` |
| `-admin-password` | 初始 admin 密码（仅 users 表为空时生效） | `admin123` |
| `-redis-url` | Redis 地址，配置后启用多实例模式 | 空（单机） |
| `-redis-prefix` | Redis 频道/键前缀（多环境共用 Redis 时隔离） | `ywm` |
| `-instance-id` | 实例标识（多实例日志/总线用） | 随机 |
| `-attach-dir` | 附件本地存储目录（置空关闭上传） | `data/attachments` |
| `-attach-quota-mb` | 每用户附件配额 MB（仅本地盘模式） | `0`（不限） |
| `-retention-days` | 会话保留天数，超期自动级联清理 | `0`（不清理） |
| `-s3-endpoint` | S3 兼容对象存储地址（配置后优先于本地盘） | 空 |
| `-oidc-issuer` / `-oidc-client-id` / `-oidc-client-secret` / `-oidc-redirect-url` | OIDC 统一认证（授权码+PKCE），四项全配才启用；redirect URL 需与 IdP 后台登记一致，形如 `https://网关域名/auth/oidc/callback` | 空 |
| `-oidc-employee-claim` | 工号所在 claim 名（缺失时回退 `sub`），按工号自动建号 | `employee_id` |

### client

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-gateway` | 网关 WebSocket URL | 配置文件值，否则 `ws://localhost:8080/ws/agent` |
| `-agent-id` | Agent 唯一标识（单 agent 模式） | 主机名 |
| `-adapter` | 本地 Agent 适配器类型：`http` 或 `stdio`（connector 模式下实例的启动命令由品牌 `launch_cmd` 下发，此项仅作兜底） | `http` |
| `-local-agent` | 本地 Agent HTTP 地址或命令路径（connector 模式下仅作品牌未配 `launch_cmd` 时的兜底） | `http://localhost:9001` |
| `-token` | 用户 JWT（通过 `node src/login.ts` 获取），随网关 `-jwt-ttl` 过期 | 无（与 `-key` 二选一） |
| `-key` | 设备密钥（`amk_` 前缀），不过期、可吊销；也可用配置文件 | 无（与 `-token` 二选一） |
| `-connector-id` | connector 模式标识（通常用主机名）。设置后 client 只起服务不带 agent 身份，agent 实例在管理后台「Agent 管理 → 注册 Agent」分配（选 connector + 品牌） | 空（单 agent 模式） |
| `-pair` | 配对码（管理后台「设备密钥」页生成）。凭码发起接入，管理员批准后密钥自动写入配置文件 | 空 |
| `-config` | connector 配置文件路径（JSON：`{gateway, connector_id, key, overrides}`） | `~/.agent-manage/connector.json` |
| `-ui-addr` | 本机管理页监听地址（绑回环不鉴权），`off` 关闭 | `127.0.0.1:9321` |
| `-log-level` | 日志级别 | `info` |
| `-task-timeout` | 任务超时 | `30m` |

**connector 零配置接入**（推荐）：管理后台生成配对码 → 目标机器执行 `node src/client.ts -pair <码> -gateway ws://网关/ws/agent` → 管理员在后台「待接入」批准 → 凭证自动写入 `~/.agent-manage/connector.json`。之后 `node src/client.ts` 零参数启动即可。该 connector 及其全部 agent 归属于配对码的生成用户。

**本机管理页**：client 启动即在 `127.0.0.1:9321` 提供管理页——未配置时页面上直接完成配对接入；已接入后可查看本机托管的 agent、按品牌添加/移除实例、为单个实例设置本机覆盖（写入配置文件 `overrides`，支持字符串=stdio 命令或对象 `{conn_type, target}`；优先级：本机覆盖 > 品牌 `conn_type`/`launch_cmd`/`endpoint` > `-local-agent`）。品牌连接方式：`stdio`（命令拉起子进程）、`http`（HTTP+SSE 服务地址）、`ws`（WebSocket 服务地址）。

> **品牌治理与审批**：管理后台「品牌管理」创建了品牌后，网关进入治理模式——注册必须带合法 `brand_id`（connector 模式由页面分配自动携带），client 主动注册的 agent 进入「待审批」，admin 批准后可用。品牌目录为空时维持旧的自由注册行为。详见 `docs/protocol.md` §10.4。

### local-agent

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-addr` | 监听地址 | `:9001` |

## 7. 常见问题

### 7.1 终端 Client 连不上网关

检查：
- 终端能否 `ping gateway.internal`
- 终端能否 `curl http://gateway.internal:8080`
- 防火墙是否放行终端到网关的出站连接
- 网关是否监听 `0.0.0.0` 而不是 `127.0.0.1`

### 7.2 用户页面不显示 Agent

检查：
- Client 是否启动成功
- Client 日志是否显示 `connecting to gateway` 且网关侧显示 `agent registered`
- 浏览器控制台是否有 WebSocket 报错
- 网关和 Client 之间网络是否连通

### 7.3 发消息后无响应

检查：
- 本地 Agent 是否启动（`curl http://localhost:9001/capabilities`）
- Client 日志是否有错误
- 本地 Agent 是否正确返回 SSE 格式

### 7.4 node 版本过低

`node --version` 必须 >= 24。低版本无法直接运行 `.ts` 文件，会报 `ERR_UNKNOWN_FILE_EXTENSION` 或语法错误，请升级 Node.js。

## 8. 监控

网关暴露两个运维端点（无需认证，生产环境建议只在内网/回环暴露，或经反代加 ACL）：

- `GET /healthz`：就绪探针。返回 `{status, db, redis, uptime_s}`；MySQL/Redis 任一不可用返回 503，可直接挂 K8s readinessProbe 或 LB 健康检查
- `GET /metrics`：Prometheus 文本格式。核心指标：
  - `ywm_agents_connected` / `ywm_users_connected` / `ywm_tasks_active`（gauge，本实例）
  - `ywm_tasks_created_total` / `ywm_tasks_completed_total` / `ywm_tasks_failed_total` / `ywm_tasks_timeout_total`
  - `ywm_task_duration_seconds_sum` / `_count`（相除即平均任务时长）
  - `ywm_messages_persisted_total`、`ywm_attachments_uploaded_total`、`ywm_attachment_bytes_total`

Prometheus 抓取示例：

```yaml
scrape_configs:
  - job_name: ywmatrix
    static_configs:
      - targets: ["gateway-1:8080", "gateway-2:8080"]
```

## 9. 限流

网关内置内存滑动窗口限流（单实例维度，多实例共享限额需换 Redis 计数器）：

| 入口 | 限额 | 维度 | 超限响应 |
|------|------|------|---------|
| `POST /auth/login` | 10 次/分钟 | 客户端 IP（取 `X-Forwarded-For` 首段） | HTTP 429 |
| `POST /attachments` | 20 次/分钟 | 用户 ID | HTTP 429 |
| `task.create` | 30 次/分钟 | 用户 ID | JSON-RPC `-32005` |

反代部署时确保 `X-Forwarded-For` 透传（`deploy/nginx.conf` 示例已含），否则所有客户端共享反代 IP 的限额。

## 10. 优雅关闭（drain）

网关收到 `SIGTERM`/`SIGINT` 后进入 drain 流程：

1. `/healthz` 立即返回 503（`status: "draining"`），LB/readinessProbe 把实例摘出
2. 拒绝新的 WS upgrade（503）；存量 agent/user 连接以 `1001` 关闭（client 有重连逻辑，多实例下自动接到健康实例）
3. 挂起的 RPC 请求收到 `server shutting down` 错误响应；缓冲区中的 assistant 消息落库；任务超时计时器停止
4. 约 1.5s 宽限后停止接受新 HTTP 连接，关闭 Redis/MySQL，退出码 0；超过 10s 未完成则强制退出（码 1）

systemd 默认即发送 SIGTERM，`deploy/agent-gateway.service` 已配 `TimeoutStopSec=15` 覆盖该窗口。滚动重启多实例时逐个 `systemctl restart`，用户侧仅有 WS 断线重连的秒级抖动。

## 11. 生产部署 checklist

- [ ] 网关部署在稳定的内网服务器，配置 systemd 自启
- [ ] 反代（Nginx/Caddy）终结 TLS，网关只监听 `127.0.0.1`（见 §3.3 与 `deploy/` 示例）
- [ ] 设置强 `AGENT_MANAGE_JWT_SECRET`、修改 admin 默认密码
- [ ] MySQL 8.0+ 就绪并限制 `ywmatrix` 账号仅本机/内网访问
- [ ] 多实例时：Redis 就绪，各实例共享同一 `JWT_SECRET`/`DATABASE_URL`/`REDIS_URL`
- [ ] 附件存储目录（或 S3 bucket）容量与备份策略明确
- [ ] 终端 Client 配置为系统服务开机自启
- [ ] 本地 Agent 有异常退出自动重启机制
- [ ] 配置审计日志落盘或入库
- [ ] 敏感操作需要终端用户二次确认
- [ ] 限制 Agent 可执行命令和访问路径
