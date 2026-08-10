# YwMatrix

Node.js (TypeScript) 实现的远程 Agent 管理平台，支持跨平台部署（Windows、macOS、Linux、麒麟桌面）。

## 文档

- [架构设计与通信机制](docs/architecture.md)
- [通信协议规范](docs/protocol.md)
- [本地 Agent 接口标准](docs/local-agent-interface.md)
- [跨平台部署指南](docs/deployment.md)

## 架构

```text
管理页面 (浏览器)  ←──WebSocket──→  网关 (gateway)  ←──→  MySQL（用户/会话/消息）
                                         │      ├──→  Redis（多实例注册表与扇出，可选）
                                         │      └──→  本地盘 / S3（附件）
                                         │ WebSocket
                                         │
终端 AgentClient (client)
    │── HTTPAdapter ──HTTP/SSE──→  本地 HTTP Agent
    │── StdioAdapter ──stdin/stdout──→  本地 CLI Agent
```

- **gateway**: 统一网关，维护 AgentClient 连接，按 agent_id 路由消息；负责认证（JWT）与会话/消息持久化；无状态，可水平扩容
- **MySQL**: 存储用户（scrypt 密码哈希）、会话、消息，按用户隔离
- **Redis（可选）**: 多实例模式下的 agent 注册表（TTL 心跳刷新）与跨实例消息扇出；不配置则为单机模式
- **client**: 跑在员工终端，内置适配器，连接网关并转发指令到本地 Agent
- **local-agent**: 本地 Agent HTTP 示例（可用可不用）
- **static/index.html**: 用户管理页面

## 环境要求

- **Node.js >= 24**：直接运行 `.ts` 文件（原生类型擦除），无需编译步骤。
- **MySQL >= 8.0**：存储用户、会话与消息。
- **Redis（可选）**：多实例部署时需要。

### 0. 准备数据库

```sql
CREATE DATABASE IF NOT EXISTS ywmatrix CHARACTER SET utf8mb4;
CREATE USER IF NOT EXISTS 'ywmatrix'@'localhost' IDENTIFIED BY 'ywmatrix_dev';
GRANT ALL PRIVILEGES ON ywmatrix.* TO 'ywmatrix'@'localhost';
```

网关首次启动会自动建表，并创建初始账号 `admin`（密码见 `-admin-password`，默认 `admin123`，请尽快修改）。

## 快速运行

### 1. 安装依赖

```bash
npm install
```

### 2. 启动各组件

```bash
# 终端 1：启动网关（生产环境务必设置 AGENT_MANAGE_JWT_SECRET）
npm run gateway

# 终端 2：启动本地 Agent（HTTP 示例）
npm run local-agent

# 终端 3：登录获取 JWT，启动终端 Client
TOKEN=$(node src/login.ts -name admin -password admin123)
node src/client.ts -adapter http -agent-id demo-mac -local-agent http://localhost:9001 -token "$TOKEN"
```

打开浏览器访问 `http://localhost:8080`，用 `admin / admin123` 登录后即可选择对应 Agent 对话。

也可以用 stdio 适配器接命令行 Agent：

```bash
node src/client.ts -adapter stdio -agent-id demo-stdio -local-agent "/path/to/your/agent" -token "$TOKEN"
```

再启动一个属于其他用户的 Agent：先在 MySQL 的 `users` 表插入该用户，然后用其 JWT 启动 client 即可，各用户只能看到自己的 Agent。

## 测试与检查

```bash
# 类型检查
npm run typecheck

# 单元测试
npm test

# 端到端冒烟测试（自动启动 gateway + local-agent + client 并走通一次任务）
./scripts/e2e.sh
```

## 部署

直接以 Node.js 运行时分发源码即可：

```bash
npm pack            # 生成 tarball，拷贝到目标机器
npm install --omit=dev
node src/gateway.ts
```

- **Windows**：可用 `nssm` 注册为系统服务，或 `start /b node src\client.ts ...`
- **Linux/麒麟**：使用 systemd service 或 `nohup node src/client.ts ... &`

如需单文件可执行程序，可使用 Node 官方的 [SEA（Single Executable Applications）](https://nodejs.org/api/single-executable-applications.html) 机制，本项目暂未内置。

## 配置

### gateway

- `-addr`: 监听地址，默认 `:8080`
- `-log-level`: 日志级别（debug/info/warn/error），默认 `info`
- `-agent-timeout`: Agent 心跳超时，默认 `90s`（毫秒值亦可）
- `-user-timeout`: 用户心跳超时，默认 `120s`
- `-task-timeout`: 任务超时，默认 `5m`
- `-database-url`: MySQL 连接串，默认 `mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix`
- `-jwt-secret`: JWT 签名密钥（**生产必须设置**，缺省时每次启动随机生成，重启后所有 token 失效）
- `-jwt-ttl`: JWT 有效期，默认 `7d`
- `-admin-password`: 初始 admin 密码（仅在 users 表为空时生效），默认 `admin123`
- `-redis-url`: Redis 地址（如 `redis://localhost:6379`）；配置后启用多实例模式（agent 注册表 + 跨实例消息扇出），缺省单机运行
- `-redis-prefix`: Redis 频道/键前缀，默认 `ywm`（测试或多套环境共用同一 Redis 时用来隔离）
- `-attach-dir`: 附件本地存储目录，默认 `data/attachments`（置空则关闭附件上传）
- `-attach-quota-mb`: 每用户附件配额（MB，仅本地盘模式生效），默认 `0` 不限
- `-retention-days`: 会话保留天数，超期会话（含消息与附件文件）每日自动清理，默认 `0` 不清理
- `-s3-endpoint`: S3 兼容对象存储地址（MinIO / OSS / S3）；配置后附件改用对象存储，优先级高于 `-attach-dir`
- `-s3-region` / `-s3-bucket` / `-s3-access-key` / `-s3-secret-key` / `-s3-public-url`: 对象存储参数，bucket 默认 `ywmatrix`
- `-oidc-issuer` / `-oidc-client-id` / `-oidc-client-secret` / `-oidc-redirect-url`: OIDC 统一认证（授权码 + PKCE），四项全配才启用；启用后登录页出现「使用统一认证登录」，按工号自动建号
- `-oidc-employee-claim`: 工号所在 claim 名，默认 `employee_id`（缺失时回退 `sub`）

附件存储两级策略：默认写本地盘并由网关 `GET /files/*` 回源；配置 `-s3-endpoint` 后走 S3 SDK（自动建 bucket、匿名可读），换云厂商 OSS 时无需改代码。两种都未启用时页面退化为消息内嵌 base64。单文件上限 20MB。

### client

- `-gateway`: 网关 WebSocket URL，默认 `ws://localhost:8080/ws/agent`
- `-agent-id`: Agent 唯一标识，默认使用主机名
- `-adapter`: 本地 Agent 适配器类型：`http` 或 `stdio`，默认 `http`
- `-local-agent`: 本地 Agent HTTP 地址或命令路径，默认 `http://localhost:9001`
- `-token`: 用户 JWT（通过 `node src/login.ts` 获取）；会随 `-jwt-ttl` 过期
- `-key`: 设备密钥（`amk_` 前缀，管理后台「设备密钥」页创建）；**长期运行的 Agent 推荐用密钥**，不过期、可吊销，与 `-token` 二选一
- `-log-level`: 日志级别，默认 `info`
- `-task-timeout`: 任务超时，默认 `5m`

### local-agent

- `-addr`: 监听地址，默认 `:9001`

以上参数均有对应的 `AGENT_MANAGE_*` 环境变量（如 `AGENT_MANAGE_ADDR`、`AGENT_MANAGE_TOKEN`），时长环境变量支持 Go 风格写法（`90s`、`5m`）。

## 用户隔离与认证

采用账号密码 + JWT 认证：

1. 用户账号存于 MySQL `users` 表，密码以 scrypt 哈希存储；首次启动自动创建 `admin`（管理员角色）
2. 页面或 CLI 通过 `POST /auth/login` 换取 JWT（HS256，默认 7 天有效）
3. 管理页面和 AgentClient 连接 WebSocket 时携带 JWT，网关从 `sub` 提取用户 ID 作为 `owner_id`；AgentClient 也可改用设备密钥（`/ws/agent?key=amk_...`，库中只存 SHA-256 哈希，明文仅创建时展示一次，吊销即时踢线）
4. 每个用户只能看到并管理自己名下的 Agent、会话和消息，越权返回 `-32004 Unauthorized`；管理员（`role=admin`）可见并管理全部 Agent
5. 管理员在独立管理后台 `/admin`（页面左下角入口，仅管理员可见）管理用户（创建/禁用/重置密码/改角色）、Agent（分页查询/断连/转移归属）与设备密钥（创建/吊销）；普通用户在「设置 → 我的设备密钥」自助管理自己的密钥，在「设置 → 修改密码」改密。禁用/改密/改角色即时踢掉旧连接

Agent 注册信息持久化在 `agents` 表（上线 upsert、离线标记、心跳节流刷新），重启网关后历史 Agent 仍可在管理后台查询。

同时支持 OIDC 统一认证（通用授权码 + PKCE 骨架）：配置 `-oidc-*` 后登录页出现「使用统一认证登录」入口；按工号（`employee_id` claim，可用 `-oidc-employee-claim` 调整）关联 `users` 表自动建号，账号密码登录保留作为兜底。

会话与消息持久化在网关侧（`sessions` / `messages` 表），换浏览器/设备不丢；浏览器 localStorage 仅作为缓存。

生产环境务必：设置强 `AGENT_MANAGE_JWT_SECRET`、修改 admin 默认密码、启用 HTTPS/WSS（由 Nginx/Caddy 反代终结 TLS，示例见 `deploy/nginx.conf` 与 `deploy/Caddyfile`，注意 `client_max_body_size` 需 ≥32m 以放行附件上传）。

## 消息协议

本项目采用基于 **JSON-RPC 2.0** 的通信协议，融合了 LSP 的流式进度通知和 MCP 的消息信封。`capabilities` 仅作为描述性标签供页面展示，本地 Agent 自行决定工具调用与确认逻辑。详见 [docs/protocol.md](docs/protocol.md) 和 [docs/local-agent-interface.md](docs/local-agent-interface.md)。

### 核心消息示例

**Agent 注册（AgentClient → Gateway）**

```json
{
  "jsonrpc": "2.0",
  "id": "reg-1",
  "method": "system.register",
  "params": {
    "agent_id": "demo-mac",
    "capabilities": [
      {"type": "chat", "name": "coding", "description": "编码助手"}
    ]
  }
}
```

**用户创建任务（User → Gateway）**

```json
{
  "jsonrpc": "2.0",
  "id": "req-1",
  "method": "task.create",
  "params": {
    "agent_id": "demo-mac",
    "task_id": "task-001",
    "type": "chat",
    "content": "帮我执行一个任务"
  }
}
```

**流式进度通知（Gateway → User page）**

```json
{
  "jsonrpc": "2.0",
  "method": "admin.task.progress",
  "params": {
    "task_id": "task-001",
    "agent_id": "demo-mac",
    "content": [{"type": "text", "text": "正在处理"}],
    "done": false
  }
}
```

**本地 Agent SSE 响应（Local Agent → AgentClient）**

```
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"task-001","type":"text","content":[{"type":"text","text":"收"}],"done":false}}
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"task-001","type":"text","content":[{"type":"text","text":"到"}],"done":false}}
data: {"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"task-001","type":"text","content":[{"type":"text","text":"！"}],"done":true}}
```
