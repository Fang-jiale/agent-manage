# 架构设计

## 1. 设计目标

本系统用于**统一远程管理运行在员工终端上的各类 Agent**（编码、办公、值班等）。核心诉求：

- 用户通过浏览器即可查看在线 Agent 并与其对话
- 每个 Agent 属于特定用户，用户只能管理自己的 Agent
- Agent 运行在员工内网终端，不需要暴露公网端口
- 支持跨平台终端（Windows、Linux、麒麟桌面）
- 实时双向通信，支持流式返回

## 2. 总体架构

```text
┌─────────────────────────────────────────────────────────────────┐
│                          用户浏览器                              │
│                      http://gateway.internal:8080               │
└───────────────────────────┬─────────────────────────────────────┘
                            │ WebSocket
                            │ /ws/admin
┌───────────────────────────▼─────────────────────────────────────┐
│                          统一网关 (gateway)                      │
│  - 维护 AgentClient 连接注册表                                   │
│  - 维护用户页面连接                                              │
│  - 按 agent_id 路由消息                                          │
│  - 提供静态管理页面                                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │ WebSocket
                            │ /ws/agent?agent_id=xxx
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐
│  员工终端 A   │    │  员工终端 B   │    │  员工终端 C   │
│ AgentClient  │    │ AgentClient  │    │ AgentClient  │
│   (client)   │    │   (client)   │    │   (client)   │
│              │    │              │    │              │
│  HTTPAdapter │    │  StdioAdapter│    │  HTTPAdapter │
│     or       │    │     or       │    │     or       │
│  StdioAdapter│    │  HTTPAdapter │    │  StdioAdapter│
└───────┬──────┘    └───────┬──────┘    └───────┬──────┘
        │                   │                   │
        │ HTTP + SSE        │ stdin/stdout      │ HTTP + SSE
        │ /capabilities     │ JSON lines        │ /capabilities
        │ /tasks            │                   │ /tasks
┌───────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐
│ 本地 Coding  │    │  本地 Office │    │  本地 Oncall │
│    Agent     │    │    Agent     │    │    Agent     │
│ （已有产品）  │    │ （命令行）   │    │ （已有产品）  │
└──────────────┘    └──────────────┘    └──────────────┘
```

## 3. 核心组件职责

### 3.1 Gateway（网关）

网关是系统的控制中心，通常部署在内网服务器上。

职责：
- 接收用户页面的 WebSocket 连接，认证并识别用户身份
- 接收终端 AgentClient 的 WebSocket 连接，绑定 Agent 到所属用户
- 维护 Agent 在线列表和能力清单
- 按用户权限过滤 Agent 列表
- 根据 `agent_id` 把用户消息路由到对应终端，并校验操作权限
- 把 Agent 的流式响应转发给所有在线用户页面
- 提供静态管理页面 `static/index.html`

### 3.2 AgentClient（终端客户端）

AgentClient 是跑在员工终端上的常驻后台程序，本质上是一个**本地 Agent 适配器**。

职责：
- 主动连接网关（只开 outbound，不监听外部端口）
- 携带用户 Token 进行认证
- 通过适配器上报本地 Agent 能力列表和所属用户
- 接收网关转发的用户指令
- 把指令转发给本地 Agent（HTTP 或 stdio）
- 把本地 Agent 的流式响应回传给网关
- 维持心跳，断线自动重连

AgentClient 内置适配器：
- **HTTPAdapter**：与本地 Agent 通过 HTTP + SSE 通信
- **StdioAdapter**：把本地 Agent 作为子进程启动，通过 stdin/stdout JSON lines 通信

### 3.3 本地 Agent

本地 Agent 是实际干活的程序，部署在员工终端上，与 AgentClient 同机运行。它**不感知网关协议**，只按约定格式与 AgentClient 交互。

本地 Agent 可以是：
- 一个已有产品（如 Claude Code 类代码 Agent）
- 一个 Python/FastAPI HTTP 服务
- 一个命令行交互式程序（stdio）
- 任何能提供约定输入输出接口的程序

AgentClient 的适配器层把不同形态的本地 Agent 统一成同一套网关协议。

## 4. 通信机制

### 4.1 三层通信链路

| 链路 | 协议 | 方向 | 用途 |
|------|------|------|------|
| 浏览器 ↔ 网关 | WebSocket | 双向 | 用户发指令、接收流式结果 |
| 网关 ↔ AgentClient | WebSocket | 双向 | 指令下发、结果上传 |
| AgentClient ↔ 本地 Agent | HTTP + SSE | 请求/流式响应 | 调用本地能力 |

### 4.2 为什么用 WebSocket

- **全双工**：用户发送指令后，Agent 可以实时推送进度和结果
- **有状态连接**：网关知道哪个 Agent 在线、掉线
- **浏览器原生支持**：管理页面直接用浏览器 WebSocket API

### 4.3 为什么 AgentClient 要主动连网关

员工终端通常处于内网、NAT 或防火墙后，无法被外部直接访问。让 AgentClient **主动 outbound 连接网关**：

- 不需要在员工电脑上开端口
- 不受 DHCP、换网、VPN 影响
- 网关只需要一个固定内网地址
- 本地 Agent 不需要具备网络连接能力

### 4.4 为什么 AgentClient 要加适配器层

不同本地 Agent 形态各异：有的是 HTTP 服务，有的是命令行程序，有的甚至有自己的 GUI。让 AgentClient 承担适配器角色：

- **本地 Agent 零侵入**：已有产品不需要改网络代码
- **统一远程协议**：网关和页面只认一种消息格式
- **灵活扩展**：新增一种本地 Agent，只需新增一个适配器
- **安全隔离**：AgentClient 可以做本地确认、命令白名单，网关不直接访问 Agent

### 4.5 本地 Agent 通信方式

AgentClient 目前支持两种本地通信方式：

| 方式 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| HTTP + SSE | 本地 Agent 是 HTTP 服务 | 跨平台、语言无关、调试方便 | 多一个本地端口 |
| stdin/stdout JSON lines | 本地 Agent 是命令行程序 | 无需端口、启动即连 | 一次只能处理一个请求 |

选择哪种方式由本地 Agent 的形态决定，AgentClient 通过 `-adapter` 参数指定。

## 5. 消息协议

所有 WebSocket 消息采用 **JSON-RPC 2.0** 格式，详见 [docs/protocol.md](protocol.md)。下面是简化示意。

### 5.1 Agent 注册

AgentClient 连接网关后，立即发送注册消息：

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

网关收到后更新 Agent 列表，并广播给用户页面：

```json
{
  "jsonrpc": "2.0",
  "method": "admin.agentList",
  "params": {
    "agents": [
      {"id": "demo-mac", "capabilities": [{"type": "chat", "name": "coding", "description": "编码助手"}]}
    ]
  }
}
```

### 5.2 用户发送指令

用户在页面选择 Agent 并发送消息：

```json
{
  "jsonrpc": "2.0",
  "id": "req-1",
  "method": "task.create",
  "params": {
    "agent_id": "demo-mac",
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "chat",
    "content": "帮我执行一个任务"
  }
}
```

### 5.3 Agent 流式响应

AgentClient 把本地 Agent 的流式输出转换为 `$/progress` 通知，逐条转发：

```json
{"jsonrpc": "2.0", "method": "$/progress", "params": {"token": "task-001", "value": {"agent_id": "demo-mac", "session_id": "session-001", "content": [{"type": "text", "text": "收到指令"}], "done": false}}}
{"jsonrpc": "2.0", "method": "$/progress", "params": {"token": "task-001", "value": {"agent_id": "demo-mac", "session_id": "session-001", "content": [{"type": "text", "text": "！"}], "done": true}}}
```

`session_id` 用于把响应和请求对应起来。

### 5.4 本地 Agent HTTP 接口

AgentClient 向本地 Agent 发送 POST 请求：

```http
POST /tasks
Content-Type: application/json

{"task_id": "task-001", "session_id": "session-001", "type": "chat", "content": "帮我执行一个任务"}
```

本地 Agent 返回 SSE 流，每个 `data:` 行是一条 JSON-RPC 通知：

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Connection: keep-alive

data: {"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "收到指令"}]}}

data: {"jsonrpc": "2.0", "method": "stream.chunk", "params": {"task_id": "task-001", "type": "text", "content": [{"type": "text", "text": "！"}], "done": true}}
```

## 6. 消息路由流程

以用户给 `demo-mac` 发送消息为例：

```text
1. 用户在浏览器输入消息
        │
        ▼
2. 浏览器通过 WebSocket 发送给网关
        │ {"jsonrpc":"2.0","id":"req-1","method":"task.create","params":{"agent_id":"demo-mac","session_id":"...","content":"..."}}
        ▼
3. 网关根据 agent_id 查找 demo-mac 的 WebSocket 连接
        │
        ▼
4. 网关把消息转发给 demo-mac 上的 AgentClient
        │ {"jsonrpc":"2.0","id":"req-1","method":"agent.chat","params":{...}}
        ▼
5. AgentClient 解析消息，转发给本地 Agent 的 /tasks 接口
        │ HTTP POST
        ▼
6. 本地 Agent 流式返回 SSE
        │
        ▼
7. AgentClient 把每个 SSE chunk 转成 `$/progress` 通知发回网关
        │
        ▼
8. 网关把响应转发给所有在线用户页面
        │
        ▼
9. 浏览器实时显示流式结果
```

## 7. 连接生命周期

### 7.1 AgentClient 连接

```text
启动 ──► 连接网关 WebSocket ──► 发送 system.register ──► 网关广播 admin.agentList
                              │
                              ├── 心跳保活（system.heartbeat）
                              ├── 接收 agent.chat 指令
                              └── 断线后重连
```

### 7.2 用户页面连接

```text
打开页面 ──► 连接 /ws/admin ──► 收到 admin.agentList ──► 选择 Agent 对话
                              │
                              ├── 发送 task.create
                              └── 接收 admin.task.progress / $/progress 流
```

### 7.3 掉线处理

- AgentClient 断线：网关从注册表移除，广播新的 admin.agentList
- 用户页面断线：网关从用户列表移除，不再向该页面推送
- 消息不持久化：Demo 版本离线消息会丢失，生产环境需要加消息队列

## 8. 并发模型

### 8.1 网关

- Node.js 单线程事件循环，所有连接共享一个进程
- 每个连接通过 `ws` 的 `message` 事件驱动，写操作直接 `ws.send()`
- 注册表为普通 `Map`，无需加锁
- 心跳超时检查由 `setInterval` 周期执行

### 8.2 AgentClient

- 主循环读取网关消息，每个 `chat` 任务以 async 函数并发处理
- 本地 Agent 的 SSE 流以异步迭代读取，并发回网关
- 任务取消通过 `AbortController` 实现

## 9. 安全设计（Demo 版本与生产建议）

### 9.1 Demo 版本已做

- AgentClient 只开 outbound，不监听外网
- 网关与 Client 之间通过 WebSocket 通信，可升级 WSS
- 用户只能管理自己的 Agent（Token 中的 user_id 即 owner_id）
- 页面敏感操作二次确认

### 9.2 生产必须补充

- **认证**：用户登录、AgentClient 接入 Token
- **授权**：明确谁能调哪些 Agent
- **终端授权**：敏感操作前弹窗让员工确认
- **审计日志**：记录所有指令和响应
- **TLS/WSS**：内网也建议启用 TLS
- **命令沙箱**：限制 Agent 可执行的命令和访问的文件范围
- **超时与熔断**：防止单个任务卡死或占用资源过久
- **适配器安全**：Stdio 适配器注意命令注入，HTTP 适配器注意 localhost 访问控制

## 10. 为什么这样设计

| 设计选择 | 优点 | 代价 |
|---------|------|------|
| AgentClient 主动连网关 | 穿越 NAT/防火墙，无需开端口 | 网关是单点，需要高可用 |
| WebSocket 全双工 | 实时、浏览器原生支持 | 连接有状态，需要心跳保活 |
| AgentClient 内置适配器 | 本地 Agent 零侵入，形态无关 | 多一层本地进程 |
| 本地 Agent 用 HTTP+SSE / stdio | 跨平台、语言无关、调试方便 | HTTP 方式多一个本地端口 |
| 网关集中路由 | 统一管理、审计方便 | 网关性能需要随规模扩展 |

这个架构在复杂度、实时性、跨平台性和对已有 Agent 的兼容性之间做了平衡，适合作为内部 Agent 管理平台的起点。
