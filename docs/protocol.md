# 通信协议规范

## 1. 设计原则

本协议用于**用户页面、网关、终端 AgentClient、本地 Agent** 四方之间的实时通信。设计时参考了以下业界成熟方案：

| 参考协议 | 借鉴点 |
|---------|--------|
| **JSON-RPC 2.0** | 请求/响应信封、`id` 关联、标准错误对象 |
| **LSP (Language Server Protocol)** | 流式进度通知（`$/progress`）、部分结果（partial results） |
| **MCP (Model Context Protocol)** | JSON-RPC 2.0 信封、能力协商、typed content |
| **A2A (Agent2Agent)** | Agent 身份标识、任务（task）语义、多轮协作 |
| **STOMP / MQTT** | 基于主题的发布订阅思想（用于 agent_list 广播） |

注意：本协议不照搬 MCP 的工具调用模型。本地 Agent 自己就是 AI，工具调用、确认、提示等由 Agent 自行决定并通过流式消息输出；`capabilities` 仅作为描述性标签供页面展示。

设计原则：
- **基于 JSON-RPC 2.0**：信封简单、成熟、易于调试
- **统一信封**：所有消息用同一格式，便于统一日志和审计
- **请求可追踪**：每个请求有唯一 `id`，响应和流式 chunk 都能对应
- **能力协商**：连接建立时交换能力，避免硬编码
- **用户隔离**：每个 Agent 属于一个用户，用户只能管理自己的 Agent
- **流式原生支持**：不依赖 HTTP SSE，WebSocket 内原生支持进度通知
- **错误标准化**：错误码、错误消息、扩展数据分离

## 2. 消息信封

所有 WebSocket 消息都是 JSON，必须符合 JSON-RPC 2.0 基本结构。

### 2.1 请求/通知

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid-or-null",
  "method": "agent.chat",
  "params": { ... }
}
```

- `jsonrpc`: 固定为 `"2.0"`
- `id`: 请求唯一标识，字符串或数字；通知（不需要响应）为 `null` 或省略
- `method`: 方法名，点分命名空间，如 `agent.chat`、`system.heartbeat`
- `params`: 方法参数，对象或数组，推荐对象

### 2.2 成功响应

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid",
  "result": { ... }
}
```

### 2.3 错误响应

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid",
  "error": {
    "code": -32600,
    "message": "Invalid Request",
    "data": {
      "detail": "missing required field 'agent_id'"
    }
  }
}
```

## 3. 认证与授权

### 3.1 登录与 JWT

账号密码通过 HTTP 接口换取 JWT：

```
POST /auth/login
Content-Type: application/json

{"name": "admin", "password": "..."}
```

响应：

```json
{"token": "eyJhbGciOi...", "user": {"id": "u-xxx", "name": "admin"}}
```

密码错误返回 `401`。密码以 scrypt 哈希存储在 `users` 表。

WebSocket 连接通过 URL Query 参数携带 JWT：

```
/ws/agent?token=eyJhbGciOi...
/ws/admin?token=eyJhbGciOi...
```

JWT 为 HS256 签名（密钥：`AGENT_MANAGE_JWT_SECRET`），`sub` 即用户 ID，有效期由 `AGENT_MANAGE_JWT_TTL` 控制（默认 7 天）。Token 缺失、签名错误或过期，网关返回 `401 Unauthorized`。

### 3.1.1 设备密钥（Device Key）

长期运行的 AgentClient 可不用 JWT，改用设备密钥接入（管理后台「设备密钥」页或 `device_key.create` 创建）：

```
/ws/agent?key=amk_...
```

- 明文形如 `amk_` + 24 字节随机数的 base64url，**只在创建时返回一次**；库中只存 SHA-256 哈希（`device_keys` 表）
- 密钥不过期；`owner_id` 取密钥属主，Agent 注册归属同 JWT 规则
- 未知/已吊销/属主被禁用的密钥统一返回裸 `401`（防探测）；`/ws/admin` 不接受设备密钥
- `token` 与 `key` 二选一（同时带 `token` 时忽略 `key`）；未配置数据库时密钥认证不可用（`401`）

### 3.2 Agent 所有权

AgentClient 连接时使用所属用户的 JWT（或设备密钥）。网关从凭证提取 `owner_id`，Agent 注册时携带的 `owner_id` 会被忽略（防止伪造）。

用户页面连接时只显示 `owner_id` 与自己 `user_id` 相同的 Agent；`admin` 角色可见全部 Agent（角色在 WS 升级时缓存到连接上，角色变更会被强制重连刷新）。

### 3.3 操作授权

用户发送 `task.create` 时，网关会检查是否有权操作目标 Agent：
- 允许操作 `owner_id` 与自己 `user_id` 相同的 Agent；`admin` 可操作任意 Agent
- 越权操作返回 `-32004 Unauthorized`
- `admin` 操作他人 Agent 时，进度通知同时发给 Agent 属主与任务发起者

## 3.4 会话与消息持久化

会话与消息存储在 MySQL（`sessions` / `messages` 表），按 `owner_id` 隔离。用户侧方法：

| 方法 | 参数 | 结果 |
|------|------|------|
| `session.list` | `{agent_id?}` | `{sessions: [{id, agent_id, title, created_at, updated_at, message_count}]}` |
| `session.create` | `{id?, agent_id, title?}` | Session 对象（`id` 缺省由服务端生成） |
| `session.rename` | `{id, title}` | `{status: "ok"}` |
| `session.delete` | `{id}` | `{status: "ok"}`（级联删除消息） |
| `message.list` | `{session_id, limit?, before?}` | `{messages: [...], total}`，`before` 为毫秒时间戳游标 |

落库时机：
- `task.create`：网关写入 `role=user` 消息（`content` 为 `{text, attachments?}`），并自动创建缺失的会话（标题取消息前 20 字）
- `$/progress`：网关在内存按 `task_id` 累积 chunk，`done`/`error`/超时时写入整条 `role=assistant` 消息（`content` 为 `{chunks: [...]}`）

### 3.5 用户管理

用户分 `admin` / `user` 两种角色（`users.role`），首次启动创建的 `admin` 账号自动为管理员。除 `user.change_password` 为自助修改外，其余仅管理员可调（否则返回 `-32004`）：

| 方法 | 参数 | 说明 |
|------|------|------|
| `user.list` | `{query?, limit?, offset?}` | `{users: [{id, name, role, disabled, created_at, last_login_at}], total}`；不带参数返回全量（兼容旧行为） |
| `user.create` | `{name, password, role?}` | 创建用户，role 默认 `user` |
| `user.disable` | `{id, disabled}` | 禁用/启用；禁用即时踢掉该用户全部连接，登录返回 401 |
| `user.reset_password` | `{id, password}` | 管理员重置密码，重置后踢掉旧连接 |
| `user.set_role` | `{id, role}` | 调整角色（`admin`/`user`）；不能改自己，变更后目标用户被踢线重连刷新缓存 |
| `user.change_password` | `{old_password, new_password}` | 自助改密（新密码至少 6 位） |

禁用账号即使持有未过期 JWT 也无法新建 WebSocket 连接（网关升级时校验）。

### 3.6 管理后台（仅 admin）

管理后台 `/admin` 页面使用的管理方法，全部要求 admin 角色：

| 方法 | 参数 | 说明 |
|------|------|------|
| `admin.overview` | `{}` | `{users_total, agents_total, agents_online, users_connected, tasks_active}` |
| `agent.list` | `{owner_id?, status?, query?, limit?, offset?}` | DB 分页（含离线历史）+ 实时状态合并，返回 `{agents: [...AgentInfo & {first_seen, last_seen, online}], total}` |
| `agent.disconnect` | `{agent_id}` | 断开本实例上的 Agent 连接（跨实例一期返回 `-32000`） |
| `agent.reassign` | `{agent_id, owner_id}` | 转移归属；改库并同步在线连接。注意 JWT 重连会按凭证夺回归属，仅对设备密钥接入的 Agent 持久 |

Agent 注册信息持久化在 `agents` 表：注册 upsert（凭证即权威，含 `owner_id`）、断连标记离线、心跳 60 秒节流刷新 `last_seen`。

### 3.7 设备密钥管理

| 方法 | 参数 | 说明 |
|------|------|------|
| `device_key.create` | `{name, owner_id?}` | 任何用户可为自己创建；`owner_id` 代他人创建仅 admin。返回 `{id, key}`，明文仅此一次。限流 10 次/分钟/用户 |
| `device_key.list` | `{owner_id?}` | 本人密钥列表（admin 可查他人）；不返回哈希/明文 |
| `device_key.revoke` | `{id}` | 吊销（本人或 admin）；使用该密钥的在线 Agent 连接即时被踢 |

### 3.8 附件上传

大文件不走 WebSocket，先通过 HTTP 上传再引用 URL：

```
POST /attachments
Authorization: Bearer <JWT>
Content-Type: application/json

{"name": "截图.png", "mime": "image/png", "data": "<base64 或 dataURL>"}
```

响应 `{"url": "/files/attachments/<uid>/<uuid>/<name>", "name", "mime", "size"}`。随后在 `task.create` 的 `metadata.attachments` 中携带 `{name, mime, size, url}`（无需再带原始数据）。单文件上限 20MB；未配置存储时返回 `503`，客户端可退化为在 `attachments[].data` 内嵌 base64。

存储后端：默认本地盘（`-attach-dir`，由网关 `GET /files/*` 回源，URL 含 UUID 不可枚举）；配置 `-s3-endpoint` 后切换为 S3 兼容对象存储（自动建 bucket 并对 `attachments/*` 开启匿名读）。

## 4. 标准错误码

| 错误码 | 名称 | 说明 |
|--------|------|------|
| `-32700` | Parse error | JSON 解析失败 |
| `-32600` | Invalid Request | 请求格式非法 |
| `-32601` | Method not found | 方法不存在 |
| `-32602` | Invalid params | 参数错误 |
| `-32603` | Internal error | 内部错误 |
| `-32000` | Agent not found | 目标 Agent 不在线 |
| `-32001` | Agent timeout | Agent 响应超时 |
| `-32002` | Task cancelled | 任务被取消 |
| `-32003` | Local agent error | 本地 Agent 执行出错 |
| `-32004` | Unauthorized | 未授权操作 |
| `-32005` | Rate limited | 触发限流（如 `task.create` 超过 30 次/分钟） |

## 5. 命名空间与方法

方法名采用 `namespace.action` 格式：

| 命名空间 | 用途 |
|---------|------|
| `system.*` | 系统级消息（心跳、注册、状态） |
| `agent.*` | Agent 相关（`agent.chat/cancel/respond` 为网关与 AgentClient 之间；`agent.list/disconnect/reassign` 为管理方法） |
| `admin.*` | 用户页面相关（页面与网关之间，命名空间保留 `admin`） |
| `user.*` | 用户管理（见 3.5） |
| `device_key.*` | 设备密钥管理（见 3.7） |
| `task.*` | 任务相关（创建、取消、进度） |

## 6. 生命周期消息

### 6.1 Agent 注册（AgentClient → Gateway）

```json
{
  "jsonrpc": "2.0",
  "id": "reg-001",
  "method": "system.register",
  "params": {
    "agent_id": "demo-mac",
    "name": "张三的 MacBook",
    "version": "1.0.0",
    "capabilities": [
      {
        "type": "chat",
        "name": "coding",
        "description": "编码助手，可读写文件、执行命令、分析代码"
      }
    ],
    "platform": {
      "os": "darwin",
      "arch": "arm64",
      "hostname": "zhangsan-mac"
    }
  }
}
```

网关响应：

```json
{
  "jsonrpc": "2.0",
  "id": "reg-001",
  "result": {
    "status": "ok",
    "server_time": "2026-07-07T12:00:00Z"
  }
}
```

### 6.2 心跳（双向通知）

AgentClient 定期发送：

```json
{
  "jsonrpc": "2.0",
  "method": "system.heartbeat",
  "params": {
    "agent_id": "demo-mac",
    "timestamp": "2026-07-07T12:00:30Z"
  }
}
```

网关可选回复：

```json
{
  "jsonrpc": "2.0",
  "method": "system.heartbeat",
  "params": {
    "timestamp": "2026-07-07T12:00:30Z"
  }
}
```

### 6.3 Agent 状态更新（AgentClient → Gateway）

```json
{
  "jsonrpc": "2.0",
  "method": "system.status",
  "params": {
    "agent_id": "demo-mac",
    "status": "busy",
    "task_id": "task-001",
    "message": "正在执行命令"
  }
}
```

## 7. 用户与 Agent 交互

### 7.1 用户创建任务（User → Gateway）

```json
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "method": "task.create",
  "params": {
    "agent_id": "demo-mac",
    "task_id": "task-001",
    "type": "chat",
    "content": "帮我执行一个任务"
  }
}
```

### 7.2 网关转发任务给 AgentClient（Gateway → AgentClient）

```json
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "method": "agent.chat",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "content": "帮我执行一个任务",
    "metadata": {
      "requester": "user-zhangsan",
      "timestamp": "2026-07-07T12:00:00Z"
    }
  }
}
```

### 7.3 AgentClient 立即响应已接收

```json
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "result": {
    "status": "accepted",
    "task_id": "task-001"
  }
}
```

### 7.4 AgentClient 上报进度（流式）

参考 LSP 的 `$/progress`，AgentClient 发送进度通知。`value.content` 推荐采用 typed content 数组，与本地 Agent 的 `stream.chunk` 保持一致：

```json
{
  "jsonrpc": "2.0",
  "method": "$/progress",
  "params": {
    "token": "task-001",
    "value": {
      "kind": "report",
      "agent_id": "demo-mac",
      "session_id": "session-001",
      "content": [{"type": "text", "text": "正在分析"}],
      "percentage": 10
    }
  }
}
```

最终完成时：

```json
{
  "jsonrpc": "2.0",
  "method": "$/progress",
  "params": {
    "token": "task-001",
    "value": {
      "kind": "end",
      "agent_id": "demo-mac",
      "session_id": "session-001",
      "content": [{"type": "text", "text": "任务执行完成"}],
      "done": true
    }
  }
}
```

### 7.5 网关转发给用户页面（Gateway → User page）

网关把 `$/progress` 转发给用户页面，`content` 保持 typed content 数组：

```json
{
  "jsonrpc": "2.0",
  "method": "admin.task.progress",
  "params": {
    "task_id": "task-001",
    "agent_id": "demo-mac",
    "session_id": "session-001",
    "content": [{"type": "text", "text": "正在分析"}],
    "percentage": 10,
    "done": false
  }
}
```

### 7.6 任务完成响应

如果任务最终需要返回结构化结果：

```json
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "result": {
    "task_id": "task-001",
    "status": "completed",
    "summary": "任务执行完成"
  }
}
```

### 7.7 用户回复确认或输入（User → Gateway）

当本地 Agent 返回 `confirm_required` 或 `prompt_required` 时，页面让用户回复：

```json
{
  "jsonrpc": "2.0",
  "id": "req-003",
  "method": "task.respond",
  "params": {
    "agent_id": "demo-mac",
    "task_id": "task-001",
    "session_id": "session-001",
    "confirm_id": "c-001",
    "response": {
      "decision": "allow",
      "message": "已核对，放行"
    }
  }
}
```

`response` 字段格式、`decision` 枚举、返回 result、多确认框并存、确认框撤销（`confirm_cancelled`）等约定，详见 [本地 Agent 接口标准 §6.2 / §8](local-agent-interface.md)。网关对此仅做透传，不解释语义。

前端层补充约定：

- 收到 `confirm_cancelled` 即关框，幂等，重复忽略；用户点击与撤销交叉时那次 `task.respond` 会返回 `-32000`，属正常不是错误。
- 收到 `task.completed` / `event.error` 时，兜底清掉该 task 下所有未回复的框。

## 8. 任务取消

### 8.1 用户取消任务（User → Gateway）

```json
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "method": "task.cancel",
  "params": {
    "agent_id": "demo-mac",
    "task_id": "task-001",
    "session_id": "session-001"
  }
}
```

`task_id` 必填，`session_id` 推荐带上。AgentClient 收到 `agent.cancel` 后向本地 Agent 补发取消的约定（stdio 子进程必须收到 `task.cancel` 否则是假停止）详见 [本地 Agent 接口标准 §6.3 / §10.3](local-agent-interface.md)。

### 8.2 网关转发取消（Gateway → AgentClient）

```json
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "method": "agent.cancel",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001"
  }
}
```

AgentClient 收到后必须做两件事：

1. 中断本地 AbortController（释放 SSE/异步队列）
2. **向本地 Agent 补发 `task.cancel`**（stdio 通过 stdin 写 JSON-RPC 通知，HTTP 取决于 LocalAgent 是否暴露 cancel 接口）

仅做 ① 不做 ② 是"假停止"——LocalAgent 和被控子进程仍继续运行、消耗资源。完整约定详见 [本地 Agent 接口标准 §6.3](local-agent-interface.md)。

### 8.3 取消完成

```json
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "result": {
    "task_id": "task-001",
    "status": "cancelled"
  }
}
```

## 9. Agent 列表广播

### 9.1 网关广播在线 Agent（Gateway → User page）

```json
{
  "jsonrpc": "2.0",
  "method": "admin.agentList",
  "params": {
    "agents": [
      {
        "id": "demo-mac",
        "name": "张三的 MacBook",
        "status": "online",
        "capabilities": [
          {"type": "chat", "name": "coding", "description": "编码助手"}
        ],
        "platform": {"os": "darwin", "arch": "arm64"}
      }
    ]
  }
}
```

### 9.2 Agent 事件通知

```json
{
  "jsonrpc": "2.0",
  "method": "admin.agent.event",
  "params": {
    "event": "offline",
    "agent_id": "demo-mac",
    "timestamp": "2026-07-07T12:05:00Z"
  }
}
```

事件类型：`online`、`offline`、`status_changed`、`capability_changed`

## 10. AgentClient 与本地 Agent 的接口

> 完整本地 Agent 接口标准见 [docs/local-agent-interface.md](local-agent-interface.md)。本文只列出最小可用子集。

AgentClient 通过**适配器**与本地 Agent 通信。适配器屏蔽了本地 Agent 的具体形态，上层网关协议保持一致。

### 10.1 通用消息格式

AgentClient 发给本地 Agent 的请求（`task.create` 的 `params`）：

```json
{
  "task_id": "task-001",
  "session_id": "session-001",
  "type": "chat",
  "content": "帮我执行一个任务"
}
```

`type` 为 `chat` 表示新消息，`respond` 表示用户对确认/反问的回复。

本地 Agent 返回的流式 chunk（JSON-RPC 通知）：

```json
{"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "收到指令"}]}}
{"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "confirm_required", "confirm_id": "c-001", "content": [{"type": "text", "text": "确认执行 rm -rf /tmp ?"}]}}
{"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "prompt_required", "prompt_id": "p-001", "content": [{"type": "text", "text": "选择文件"}], "options": ["a.txt", "b.txt"]}}
{"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "完成"}], "done": true}}
```

chunk `type` 说明：

| type | 含义 |
|------|------|
| `text` | 普通文本输出 |
| `thinking` | 思考过程 |
| `action` | Agent 内部动作记录（原 `tool_use`） |
| `result` | 动作结果（原 `tool_result`） |
| `confirm_required` | 需要用户确认 |
| `prompt_required` | 需要用户输入/选择 |
| `block_required` | 需要用户填写表单/复杂交互 |
| `confirm_cancelled` | 待决确认被系统撤销（用户没点但 Agent 已放弃），通知类 `id:null`，前端关框 |

各 chunk 的字段细节、`task.respond` 的 response 对象格式与返回值、多确认框并存规则、stdio 取消转发等完整约定，见 [本地 Agent 接口标准](local-agent-interface.md)。

### 10.2 HTTP 适配器

本地 Agent 暴露 HTTP 接口：

```http
GET /capabilities

HTTP/1.1 200 OK
Content-Type: application/json

{
  "capabilities": [
    {"type": "chat", "name": "coding", "description": "编码助手"}
  ]
}
```

```http
POST /tasks
Content-Type: application/json

{
  "task_id": "task-001",
  "session_id": "session-001",
  "type": "chat",
  "content": "帮我执行一个任务"
}
```

返回 SSE（每个 `data:` 行是一条 JSON-RPC 通知）：

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream

data: {"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "收到指令"}], "done": false}}

data: {"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "完成"}], "done": true}}
```

### 10.3 Stdio 适配器

本地 Agent 作为子进程启动，AgentClient 通过 stdin/stdout JSON lines 通信。

AgentClient 向 stdin 写入：

```text
{"type":"chat","task_id":"task-001","session_id":"session-001","content":"帮我执行一个任务"}
```

本地 Agent 向 stdout 写入 JSON-RPC 通知：

```text
{"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"task-001","type":"text","content":[{"type":"text","text":"收到指令"}]}}
{"jsonrpc":"2.0","method":"stream.chunk","params":{"task_id":"task-001","type":"confirm_required","confirm_id":"c-001","content":[{"type":"text","text":"确认执行吗？"}]}}
```

当本地 Agent 返回 `confirm_required` 或 `prompt_required` 时，本次请求读取暂停，等待用户通过 `task.respond` 回复后继续。

**取消必须显式转发**：AgentClient 收到 `agent.cancel` 后只中断本地 AbortController 是不够的（shim 与子进程仍在跑、继续烧 token）。stdio 适配器在 AbortSignal 触发时必须额外通过 stdin 写一条 `task.cancel` 通知给本地 Agent。详见 [本地 Agent 接口标准 §6.3](local-agent-interface.md)。

## 11. 消息时序图

### 11.1 用户发送聊天消息

```text
User           Gateway          AgentClient       LocalAgent
 │               │                  │                 │
 │─task.create──▶│                  │                 │
 │               │──agent.chat─────▶│                 │
 │               │◀─result:accepted─│                 │
 │               │                  │────请求─────────▶│
 │               │                  │◀────流式输出─────│
 │               │◀──$/progress─────│                 │
 │◀─admin.task.─────────────────────│                 │
 │    progress                     │                  │
 │               │                  │◀──confirm_required
 │               │◀──$/progress:confirm_required       │
 │◀─admin.task.─────────────────────│                 │
 │   progress:confirm_required     │                  │
 │─task.respond─▶│                  │                 │
 │               │──agent.respond──▶│                 │
 │               │                  │────回复─────────▶│
 │               │                  │◀────继续输出─────│
 │               │◀──$/progress─────│                 │
 │◀─admin.task.─────────────────────│                 │
 │    progress:done                │                  │
```

## 12. 与业界方案对比

### 12.1 为什么不用纯 MCP？

MCP 主要解决**外部 AI 调用本地工具/资源**的问题，重点是工具发现、schema 定义和外部编排。本系统中**本地 Agent 自己就是 AI**，工具调用、确认、提示等由 Agent 自行决定；远程页面只负责聊天和展示。因此本协议借鉴 MCP 的 JSON-RPC 信封和 typed content，但不使用其工具调用模型。

### 12.2 为什么不用纯 JSON-RPC？

JSON-RPC 2.0 本身不支持流式。本系统借鉴 LSP 的 `$/progress` 通知，在 JSON-RPC 基础上扩展了原生流式能力。

### 12.3 为什么不用 A2A？

A2A 还处于早期，且偏向 Agent 之间的协作。本系统当前主要是**人与 Agent 的交互**，未来 Agent 互操作时再引入 A2A 更合适。

## 13. 扩展建议

- **消息压缩**：大消息可启用 per-message deflate
- **二进制数据**：文件传输建议用独立 HTTP 接口，不在 WebSocket 里传大文件
- **鉴权字段**：在 `system.register` 和每个请求中增加 `auth.token`
- **端到端加密**：敏感内容可在 AgentClient 与管理页面之间加密，网关只透传密文
- **批处理**：未来可支持 JSON-RPC batch，一次发送多个请求
