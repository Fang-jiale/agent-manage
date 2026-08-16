# 本地 Agent 接口标准

## 1. 设计目标

本标准定义 **AgentClient** 与 **本地 Agent** 之间的通信规范。设计时参考以下业界成熟方案：

| 参考方案 | 借鉴点 |
|---------|--------|
| **MCP (Model Context Protocol)** | JSON-RPC 2.0 信封、`initialize` 能力协商、typed content（text/image/resource） |
| **A2A (Agent2Agent)** | Agent Card 发现、`Task` / `Message` / `Part` / `Artifact` 数据模型、`contextId` 会话分组、SSE 流式 |
| **LSP (Language Server Protocol)** | 流式进度通知（`$/progress`）、请求可追踪 |
| **JSON-RPC 2.0** | 请求/响应/通知信封、`id` 关联、标准错误对象 |
| **Rocket.Chat UI Kit / Slack Block Kit** | 结构化消息块（section、actions、input）、交互元素（button、select、text input）、`actionId` 回传 |

本标准与 MCP 的关键区别：MCP 是外部 AI 调用本地工具的协议；而本场景中**本地 Agent 自己就是 AI**，外部只是聊天页面。因此能力声明是**描述性标签**（让页面知道 Agent 类型），不是可被远程调用的工具接口；工具调用、确认、提示等均由本地 Agent 自行决定并通过流式消息输出。

设计原则：

- **传输无关**：同一语义可映射到 HTTP+SSE、stdio JSONL、WebSocket、Unix Socket
- **语言无关**：基于 JSON，不依赖特定语言特性
- **能力协商**：Agent 主动声明自己的能力、Schema、权限
- **流式原生**：任务执行结果以流式 chunk 返回
- **双向通信**：不仅 AgentClient 可向 Agent 发任务，Agent 也可主动上报状态、事件、能力更新
- **会话可追踪**：每个任务有唯一 `task_id`，每次连续对话有唯一 `session_id`

## 2. 传输层绑定

### 2.1 HTTP + SSE

**适用场景**：本地 Agent 是常驻 HTTP 服务（Python/FastAPI/Go/Node.js 等）。

- AgentClient 通过 `GET /capabilities` 发现能力（返回描述性标签，见 5.2 注册消息结构）
- AgentClient 通过 `POST /tasks` 下发任务；请求体即 `task.create` 的 `params`
- Agent 通过 `text/event-stream` 返回流式结果，每个 `data:` 行是一条 JSON-RPC 通知（如 `stream.chunk`）
- AgentClient 在用户点停止 / task 超时时会 `POST /tasks/:id/cancel`；Agent 应早停当前任务并兜底发完 `confirm_cancelled`（若有挂起确认）+ `done:true` chunk（reason: `task_cancelled`）。未实现的旧 Agent 不影响取消——SSE 流会被 client 主动断开，仅终止态 chunk 丢失
- 可选 `GET /health` 健康检查

对于持久连接（如 WebSocket 或长连接 SSE），连接建立后需先进行 `lifecycle.initialize` 能力协商；单次 HTTP 调用场景可通过 `GET /capabilities` 完成静态发现。

> 注意：本协议不暴露可被外部调用的工具接口。本地 Agent 自行决定何时调用工具、何时请求用户确认；这些决策结果通过 `stream.chunk` 的各种 `type` 输出给页面展示。

优点：跨平台、语言无关、调试方便、可独立升级。

### 2.2 Stdio JSON Lines

**适用场景**：本地 Agent 是命令行交互式程序（Claude Code 类 Agent）。

- AgentClient 将 Agent 作为子进程启动
- **连接建立后，AgentClient 先发送 `lifecycle.initialize`**，Agent 响应能力集合，随后 AgentClient 发送 `lifecycle.initialized` 通知
- AgentClient 向 Agent 的 stdin 写入 JSON 行（请求/通知）
- Agent 向 stdout 写入 JSON 行（响应/通知）
- stderr 可用于日志，AgentClient 可选择性收集

优点：无需端口、启动即连、适合已有 CLI 工具。

### 2.3 WebSocket（可选）

**适用场景**：本地 Agent 本身支持 WebSocket（如 Electron 应用内部服务）。

- AgentClient 作为 WebSocket 客户端连接本地 Agent
- 双向 JSON 消息（单条 JSON，非 JSONL）；其余消息语义与 stdio 绑定一致（`lifecycle.initialize` 协商、`stream.chunk` 按 `task_id` 路由、取消时补发 `task.cancel`）
- 当前实现：AgentClient 内置 `ws` 适配器（`src/adapters/ws.ts`），品牌 `conn_type: "ws"` + `endpoint` 即走此通道

优点：真正的全双工，Agent 可随时主动推送。

### 2.4 传输选择建议

| Agent 形态 | 推荐传输 |
|-----------|---------|
| HTTP 服务 | HTTP + SSE |
| 命令行程序 | Stdio JSONL |
| Electron/GUI | HTTP + SSE 或 WebSocket |
| 长驻后台进程 | HTTP + SSE 或 WebSocket |

## 3. 消息信封

所有消息为 JSON，**必须**采用 JSON-RPC 2.0 基本结构。连接建立后，AgentClient 必须先发送 `lifecycle.initialize` 进行能力协商；协商完成后本地 Agent 再发送 `lifecycle.register` 完成注册（与 MCP 的 `initialize` → `notifications/initialized` → 正常运行流程对齐）。

- 请求/响应必须携带 `id`；通知（不需要响应）的 `id` 为 `null` 或省略。
- 双方只能使用对方在 `capabilities` 中声明过的能力。

### 3.1 请求/通知

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid-or-null",
  "method": "task.create",
  "params": { ... }
}
```

### 3.2 响应

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid",
  "result": { ... }
}
```

### 3.3 错误

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid",
  "error": {
    "code": -32602,
    "message": "Invalid params",
    "data": { "detail": "missing required field 'task_id'" }
  }
}
```

## 4. 命名空间与方法

方法名采用 `domain.action` 格式：

| 命名空间 | 用途 |
|---------|------|
| `lifecycle.*` | 注册、心跳、状态、能力更新 |
| `task.*` | 任务创建、回复、取消、完成、编排发起（`task.invoke`，见 §6.6） |
| `stream.*` | 流式输出块（含 text/thinking/action/result/confirm_required/prompt_required/block_required） |
| `event.*` | Agent 主动事件 |

## 5. 生命周期消息

### 5.1 初始化与能力协商 (lifecycle.initialize)

参考 MCP，连接建立后 AgentClient 必须先发送 `lifecycle.initialize`，双方交换协议版本与能力。本地 Agent 返回支持的能力后，AgentClient 发送 `lifecycle.initialized` 通知，正式进入正常运行。

**AgentClient → Agent**

```json
{
  "jsonrpc": "2.0",
  "id": "init-1",
  "method": "lifecycle.initialize",
  "params": {
    "protocolVersion": "1.0.0",
    "capabilities": {
      "chat": {},
      "tools": { "listChanged": true },
      "streaming": {},
      "confirmations": {},
      "prompts": {}
    },
    "clientInfo": {
      "name": "agent-client",
      "version": "1.0.0"
    }
  }
}
```

**Agent → AgentClient**

```json
{
  "jsonrpc": "2.0",
  "id": "init-1",
  "result": {
    "protocolVersion": "1.0.0",
    "capabilities": {
      "chat": {},
      "tools": { "listChanged": true },
      "streaming": {},
      "confirmations": {},
      "prompts": {}
    },
    "serverInfo": {
      "name": "coding-agent",
      "version": "1.2.0"
    }
  }
}
```

**AgentClient → Agent（通知，无 id）**

```json
{
  "jsonrpc": "2.0",
  "method": "lifecycle.initialized"
}
```

### 5.2 Agent 注册 (lifecycle.register)

初始化完成后，本地 Agent 发送注册消息，上报完整身份、能力标签、平台信息。AgentClient 将其翻译为网关 `system.register`。

这里的 `capabilities` 是**描述性标签**，供页面展示和按类型过滤，不是可被远程调用的工具接口。本地 Agent 内部的工具调用、确认逻辑由它自己决定。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "lifecycle.register",
  "params": {
    "agent_id": "coding-agent",
    "name": "代码助手",
    "version": "1.2.0",
    "description": "帮助用户编写、重构、审查代码",
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

### 5.3 心跳 (lifecycle.ping)

AgentClient 可定期向 Agent 发送心跳，Agent 应答。

```json
// AgentClient → Agent
{
  "jsonrpc": "2.0",
  "id": "ping-1",
  "method": "lifecycle.ping",
  "params": { "timestamp": "2026-07-08T12:00:00Z" }
}

// Agent → AgentClient
{
  "jsonrpc": "2.0",
  "id": "ping-1",
  "result": { "status": "ok", "timestamp": "2026-07-08T12:00:00Z" }
}
```

### 5.4 状态更新 (lifecycle.status)

Agent 主动上报当前状态。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "lifecycle.status",
  "params": {
    "status": "busy",
    "task_id": "task-001",
    "session_id": "session-001",
    "message": "正在执行命令"
  }
}
```

状态枚举：`idle`、`busy`、`error`、`offline`。

### 5.5 能力更新 (lifecycle.capabilities_updated)

Agent 在运行中能力发生变化时主动通知。等价于 MCP 的 `notifications/tools/list_changed` / `notifications/resources/list_changed`。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "lifecycle.capabilities_updated",
  "params": {
    "capabilities": [ ... ]
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "lifecycle.capabilities_updated",
  "params": {
    "capabilities": [ ... ]
  }
}
```

## 6. 任务消息

### 6.1 创建任务

```json
// AgentClient → Agent
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "method": "task.create",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "context_id": "ctx-conversation-abc",
    "type": "chat",
    "content": "帮我重构 main.go",
    "history": [
      { "role": "user", "content": "帮我重构 main.go" }
    ],
    "reference_task_ids": [],
    "requester": "zhangsan",
    "timestamp": "2026-07-08T12:00:00Z",
    "timeout": 300,
    "metadata": {
      "source": "web"
    }
  }
}

// Agent → AgentClient
{
  "jsonrpc": "2.0",
  "id": "req-001",
  "result": {
    "task_id": "task-001",
    "status": "accepted"
  }
}
```

> **`session_id` 产生约定（当前实现）**：由 **AgentClient（网关）** 在 `session.create` 时用 `crypto.randomUUID()` 生成合法 UUID，写入持久化存储；后续 `task.create` 始终携带该值，Agent 透传给被控子进程（如 ywcoder `--resume <uuid>`）即可。
>
> 关键要求：到达子进程的 `session_id` 必须是合法 UUID，否则续接能力丢失（被控端退化为「一次性会话」）。当前链路下浏览器永远携带网关生成的 UUID，此约束已满足。
>
> Agent 不应自行生成并回填 `session_id`——AgentClient 不消费 ack 里的该字段，Agent 自行回填只会造成「真相分裂」（AgentClient 与被控子进程各持一套 id）。

#### 6.1.2 会话工作目录（`metadata.workdir`）

会话可绑定一个工作目录（用户在页面上设置，`session.set_workdir`）。绑定后，该会话的每条消息（含群聊 fan-out 与管理者编排的子任务）网关都会在 `metadata.workdir` 注入目录路径：

```json
"metadata": {
  "workdir": "/Users/zhangsan/project/foo"
}
```

- 目录绑定在**会话**上而非 Agent 实例上：同一 Agent 的不同会话可以在不同目录下干活。
- Agent 应在每个任务开始时读取该字段并在对应目录下执行（stdio 子进程自身 cwd 是 spawn 时定死的，Agent 需在内部把目录传给被控工具/进程）。
- 未绑定的会话没有该字段，Agent 行为不变（跟随实例启动目录）。
- 与实例级配置的关系：品牌 `launch_cmd` / 本机 override 里的目录写法（一目录一实例）仍然有效；`metadata.workdir` 优先级由 Agent 自行决定。

#### 6.1.1 群聊上下文（`metadata.group`）

消息来自群聊时，网关会在 `metadata.group` 注入群上下文（单 Agent 会话没有该字段；AgentClient 原样透传）：

```json
"metadata": {
  "group": {
    "group_id": "g-xxx",
    "group_name": "调研群",
    "manager_agent_id": "leader-1",
    "members": [
      {"agent_id": "leader-1", "name": "张三的 MacBook"},
      {"agent_id": "worker-a", "name": "Mac mini"}
    ],
    "mentions": ["leader-1"]
  }
}
```

- `manager_agent_id`：群管理者（leader）。等于自身 agent_id 时，本 Agent 可通过 AgentClient 发起 `agent.task.invoke` 调度群内其他成员（见网关协议 §10.6）。
- `members`：全部成员及显示名（含离线成员），即群的规模与花名册。
- `mentions`：本条消息实际命中的目标（用户 `@全体` 时已展开为成员列表；管理者编排子任务时为被派发的目标）。

建议 Agent 实现把该结构写进 system prompt（如「你在群聊 X 中，成员有 N 人：…；你是/不是管理者」），即可自主判断是否需要分工协作。不识别该字段的 Agent 行为不变。

### 6.2 用户回复

当 Agent 输出 `confirm_required`、`prompt_required` 或 `block_required` 后，AgentClient 将用户回复转发给 Agent。

#### 6.2.1 confirm / prompt 的 response 格式

对 `confirm_required`，`response` 必须是决策对象：

```json
// AgentClient → Agent
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "method": "task.respond",
  "params": {
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

`decision` 枚举：

| 值 | 含义 |
|---|---|
| `allow` | 同意执行该工具/操作 |
| `deny` | 拒绝；Agent 收到后应让被控子进程走"被拒"分支而不是失败 |
| `cancel` | 中止整个任务，效果等同 §6.3 `task.cancel`，只是入口在确认框上 |

> **前端约定**：弹窗的「×」/「关闭」按钮应映射为 `deny`（只拦这一步、对话继续），只有明确的「终止任务」按钮才发 `cancel`。否则用户随手关一个确认框就会让长任务前功尽弃。

`message` 可选，用于填拒绝理由等。Agent 必须接受不带 `message` 的对象。

旧版兼容：仍可能收到 `boolean`（`true`↔`allow`、`false`↔`deny`）或自由字符串，Agent 自行归一为 `allow/deny/cancel`。

对 `prompt_required`，`response` 仍是字符串或字符串数组（用户选择）；对 `block_required`，是表单结果对象。

#### 6.2.2 task.respond 返回值

原 `id` 回传，返回归一后的裁决，便于 AgentClient/前端核对"用户点的被理解成了什么"：

```json
// Agent → AgentClient
{
  "jsonrpc": "2.0",
  "id": "req-002",
  "result": {
    "task_id": "task-001",
    "session_id": "session-001",
    "confirm_id": "c-001",
    "status": "accepted",
    "decision": "allow"
  }
}
```

- 以通知形式发（不带 `id`）时，Agent 照常执行但不回 `result`。
- `confirm_id` 不存在 / 已回复 / 已撤销 → 错误码 `-32000`，AgentClient 不应将其视为前端错误。

### 6.3 取消任务

```json
// AgentClient → Agent
{
  "jsonrpc": "2.0",
  "method": "task.cancel",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001"
  }
}
```

通知类（不带 `id`），Agent 不回 result。`task_id` 必填，`session_id` 可选但建议带上。

**stdio 适配器必须显式转发**：AgentClient 收到网关的 `agent.cancel` 后只中断本地 AbortController 是不够的——子进程仍在跑、继续烧 token，"停止"是假的。stdio 适配器在 AbortSignal 触发时必须额外通过 stdin 写一条 `task.cancel` 通知给子进程。子进程收到后必须：

1. 中断被控工具（如 ywcoder）的当前操作；
2. 补发 `confirm_cancelled`（reason=`task_cancelled`）清掉所有待决确认框；
3. 结束本轮。

若 stdin 已关闭（子进程退出），跳过补发。HTTP 适配器则视本地 Agent 实现是否暴露 cancel 接口而定，建议同样补发。

由于审批不设超时，"用户点停止"是待决任务**唯一**的退出路径——用户不点，子进程会一直等。可另保留一个较长任务超时（默认 30 分钟，可配 `-task-timeout`）作为兜底。

### 6.4 任务完成

Agent 主动通知任务完成。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "task.completed",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "status": "completed",
    "summary": "已完成 main.go 重构"
  }
}
```

### 6.5 斜杠命令（Slash Commands）

斜杠命令（`/model`、`/compact`、技能等）**不是协议层概念**：链路上它就是一次普通的 `task.create`，网关与 AgentClient 原样透传、零改动；命令的发现、解析、执行全部收敛在本地 Agent 一处。

**发现：用 capabilities 声明命令。** Agent 在 `lifecycle.register`（§5.2）或 `lifecycle.capabilities_updated`（§5.5）中上报 `type: "command"` 的能力项；`metadata.args` 描述参数规格，供页面渲染补全菜单：

```json
{
  "type": "command",
  "name": "model",
  "description": "切换模型",
  "metadata": {
    "current": "kimi-k2",
    "args": [
      { "name": "model", "type": "enum", "options": ["kimi-k2", "kimi-k2-thinking"], "required": true }
    ]
  }
}
```

- 每个技能注册为一条 command，并以 `metadata.kind: "skill"` 与普通命令区分（页面据此分组显示）。
- `metadata.current` 回写当前值（如当前模型），页面在会话栏展示；值变化时随 `capabilities_updated` 推送。
- capabilities 为**全量替换**语义：技能增删、current 变化都重推完整列表；未变化不要推（每次推送触发一次全量 agent 列表广播）。

**执行：`metadata.command` + 斜杠文本双通道。** 页面把命令作为普通任务发送，`content` 是人类可读的斜杠文本（进历史、供旧 Agent 解析），`metadata.command` 是结构化参数（免字符串解析）：

```json
{
  "method": "task.create",
  "params": {
    "task_id": "task-002",
    "session_id": "session-001",
    "type": "chat",
    "content": "/model kimi-k2-thinking",
    "metadata": {
      "command": { "name": "model", "args": { "model": "kimi-k2-thinking" } }
    }
  }
}
```

- `args` 中枚举参数按 `metadata.args` 里的 `name` 填；命令后的剩余自由文本（如 `/commit 只提交 src 目录`）放在 `args.text`。
- Agent 应优先读 `metadata.command`；不存在时退化为解析 `content` 的 `/` 前缀（用户手输、旧页面都走这条兜底）。
- 未在 capabilities 中声明的 `/xxx` 文本也会原样送达，Agent 可自行决定是否识别。

**执行语义由 Agent 自定**：本地命令（如切模型）通常不调用 LLM，直接生效后输出一条 text chunk（如「已切换模型：kimi-k2 → kimi-k2-thinking」）并正常结束任务；模型、技能等状态按 `session_id` 维护，切换只影响当前会话。

### 6.6 管理者编排（task.invoke / task.subtask_result）

群聊中若 `metadata.group.manager_agent_id`（§6.1.1）等于自身 agent id，Agent 是本群**管理者**，可以把群内其他成员作为子任务调度。发起与结果回收都经 AgentClient 桥接到网关（鉴权、归因、防递归在网关完成，见网关协议 §10.6）。

**发起（Agent → AgentClient，请求）**：

```json
{
  "jsonrpc": "2.0",
  "id": "inv-local-1",
  "method": "task.invoke",
  "params": {
    "parent_task_id": "task-001",
    "group_id": "g-xxx",
    "target_agent_id": "worker-a",
    "type": "chat",
    "content": "查一下数据",
    "metadata": {}
  }
}
```

- `parent_task_id`：自己正在处理的任务 id（子任务挂在父任务下，群里可见）。
- `target_agent_id`：群内成员（不能是自己）。
- AgentClient 转成网关 `agent.task.invoke`，把网关的 result / error（如 `-32006` 非管理者、嵌套编排）**原样透传**回来：

```json
{ "jsonrpc": "2.0", "id": "inv-local-1", "result": { "task_id": "task-001@ab12cd34", "status": "dispatched" } }
```

**结果回推（AgentClient → Agent，notification）**——子任务终结（completed/failed/timeout）时：

```json
{
  "jsonrpc": "2.0",
  "method": "task.subtask_result",
  "params": {
    "task_id": "task-001@ab12cd34",
    "parent_task_id": "task-001",
    "group_id": "g-xxx",
    "target_agent_id": "worker-a",
    "status": "completed",
    "chunks": [{ "type": "text", "text": "调研结果…" }],
    "error": null
  }
}
```

约束：

- 仅 **stdio / ws 适配器**支持（需要 AgentClient → Agent 的下行通道）；HTTP 适配器下 `task.invoke` 不可用。
- 编排深度硬限 1 层：子任务处理中再 invoke 会得到 `-32006`。
- 子任务与父任务必须同在一条 AgentClient 连接上（多实例部署时网关拒绝跨实例编排）。
- 网关断线期间未决的 `task.invoke` 会收到 `-32603 gateway connection lost` 错误响应。

## 7. 流式输出消息

Agent 执行任务时，通过流式 chunk 返回进度和结果。**这些 chunk 都是本地 Agent 自己决定输出的内容形态**：普通文本、思考过程、内部动作记录、需要用户确认/输入等。远程页面只负责渲染和回传，不干预 Agent 内部决策。

每个 chunk 的 `content` 字段推荐采用 **typed content** 数组（参考 MCP 内容类型），便于前端统一渲染文本、图片、资源、代码块等。

### 7.1 文本块

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "text",
    "content": [
      { "type": "text", "text": "我先看看 main.go 的结构" }
    ],
    "percentage": 10,
    "done": false
  }
}
```

### 7.2 思考过程

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "type": "thinking",
    "content": [
      { "type": "text", "text": "这个函数有点长，应该拆分..." }
    ]
  }
}
```

### 7.3 Agent 内部动作记录

本地 Agent 如果希望向页面展示自己执行了某个内部动作（如读取文件、执行命令），可以输出 `action` 类型。`name` 是 Agent 内部定义的任意标识，不是协议规定的工具名，页面仅做展示。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "type": "action",
    "name": "inspect_source",
    "arguments": {
      "path": "main.go",
      "limit": 50
    }
  }
}
```

### 7.4 动作结果

动作结果返回 typed content 数组，可包含文本、图片、资源引用等。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "type": "result",
    "name": "inspect_source",
    "content": [
      { "type": "text", "text": "package main\n..." }
    ]
  }
}
```

图片示例：

```json
{
  "type": "image",
  "data": "iVBORw0KGgoAAAANSUhEUgAAAAE...",
  "mimeType": "image/png"
}
```

资源引用示例：

```json
{
  "type": "resource",
  "resource": {
    "uri": "file:///tmp/build.log",
    "mimeType": "text/plain",
    "text": "build output..."
  }
}
```

### 7.5 代码块

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "type": "code",
    "language": "go",
    "content": [
      { "type": "text", "text": "func handleChat(...) { ... }" }
    ]
  }
}
```

### 7.6 Artifact（任务产物）

参考 A2A，任务最终可产生一个或多个 Artifact（文件、图片、结构化数据）。Artifact 通过 `stream.artifact` 通知发送，可增量传输。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.artifact",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "artifact_id": "artifact-refactor-v1",
    "name": "main.go",
    "description": "重构后的 main.go",
    "parts": [
      {
        "type": "resource",
        "resource": {
          "uri": "file:///home/user/project/main.go",
          "mimeType": "text/x-go",
          "text": "package main\n..."
        }
      }
    ],
    "append": false,
    "last_chunk": true
  }
}
```

## 8. 交互型流式内容

当本地 Agent 自己判断需要用户确认、输入或填写表单时，会输出交互型内容。这些不是外部协议强制 Agent 调用的接口，而是 Agent 内部决策后向页面展示的形态。

交互型内容可以作为 `stream.chunk` 的一种 `type`，也可以作为独立通知 `interaction.*` 发送。推荐实现：

- 简单确认/选择：`stream.chunk` with `type: "confirm_required"` / `"prompt_required"`
- 复杂表单：`interaction.block_required` 独立通知

### 8.1 确认请求

Agent 决定执行敏感操作前，请求用户确认。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "confirm_required",
    "confirm_id": "c-001",
    "title": "执行危险命令",
    "content": [
      { "type": "text", "text": "即将执行：rm -rf /tmp/old-builds" }
    ],
    "level": "dangerous"
  }
}
```

`level` 枚举：`info`、`warning`、`dangerous`。

> `timeout` 字段在 schema 中可选，但 **shim 不发送**，缺省表示无限等待审批；**前端不得自行设置默认超时或倒计时自动关闭确认框**。用户从手机点进来需要十几分钟是正常情况。

**多确认框并存**：Agent 一轮里可能并发请求多个工具的权限，会同时推多条 `confirm_required`，各自 `confirm_id` 独立、可任意顺序回复。AgentClient/前端必须支持 N 个待确认项并存，按 `confirm_id` 路由响应；不维护"当前 confirm"全局态。

#### 8.1.1 确认框撤销（confirm_cancelled）

场景：用户还没点，但 Agent 已放弃该权限请求。Agent 必须补发一条 `confirm_cancelled` 通知，否则前端框会一直挂着。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "confirm_cancelled",
    "confirm_id": "c-001",
    "reason": "task_cancelled"
  }
}
```

`reason` 枚举：

| 值 | 含义 |
|---|---|
| `task_cancelled` | 用户点了停止（参见 §6.3） |
| `interrupted` | 同轮其它操作触发中止 |
| `agent_exited` | 被控子进程异常退出，shim 兜底补发 |

约定：

- 通知类消息（`id:null`），AgentClient 不回。
- 前端幂等：收到就关框，重复忽略。
- 用户点击与撤销交叉时，那次 `task.respond` 会返回 `-32000`，属正常不是错误。
- 前端收到 `task.completed` / `event.error` 时，必须兜底清掉该 task 下所有未回复的框。

### 8.2 输入/选择请求

Agent 需要用户输入或选择时发送。

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "prompt_required",
    "prompt_id": "p-001",
    "content": [
      { "type": "text", "text": "选择要重构的函数" }
    ],
    "options": ["handleChat", "doSendMessage", "sendProgress"],
    "allow_free_text": true
  }
}
```

### 8.3 富交互块（Block Kit）

参考 Rocket.Chat UI Kit / Slack Block Kit，当简单文本/按钮不足以表达交互时，Agent 可返回结构化 block。前端按 block 类型渲染，用户操作后通过 `task.respond` 回传（`response` 中携带 block 结果）。

**Agent → AgentClient**

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "stream.chunk",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "type": "block_required",
    "block_id": "b-001",
    "blocks": [
      {
        "type": "section",
        "text": { "type": "mrkdwn", "text": "请确认部署参数" }
      },
      {
        "type": "input",
        "block_id": "env-block",
        "element": {
          "type": "static_select",
          "action_id": "select-env",
          "placeholder": "选择环境",
          "options": [
            { "text": "测试", "value": "test" },
            { "text": "生产", "value": "prod" }
          ]
        },
        "label": { "type": "plain_text", "text": "环境" }
      },
      {
        "type": "actions",
        "elements": [
          {
            "type": "button",
            "action_id": "confirm-deploy",
            "text": "确认部署",
            "style": "danger",
            "value": "deploy"
          },
          {
            "type": "button",
            "action_id": "cancel-deploy",
            "text": "取消",
            "value": "cancel"
          }
        ]
      }
    ]
  }
}
```

**AgentClient → Agent：用户提交 block 操作**

```json
{
  "jsonrpc": "2.0",
  "id": "req-004",
  "method": "task.respond",
  "params": {
    "task_id": "task-001",
    "session_id": "session-001",
    "block_id": "b-001",
    "action_id": "confirm-deploy",
    "response": {
      "env-block": { "select-env": "prod" }
    }
  }
}
```

## 9. 事件消息

Agent 主动通知非任务相关事件。

### 9.1 通知

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "event.notification",
  "params": {
    "level": "warning",
    "title": "磁盘空间不足",
    "content": "D 盘剩余空间不足 1GB"
  }
}
```

### 9.2 错误

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "method": "event.error",
  "params": {
    "task_id": "task-001",
    "code": "LOCAL_AGENT_ERROR",
    "message": "命令执行失败：权限不足",
    "recoverable": false
  }
}
```

## 10. 错误码

| 错误码 | 名称 | 说明 |
|--------|------|------|
| `-32700` | Parse error | JSON 解析失败 |
| `-32600` | Invalid Request | 请求格式非法 |
| `-32601` | Method not found | 方法不存在 |
| `-32602` | Invalid params | 参数错误 |
| `-32603` | Internal error | 内部错误 |
| `-32000` | Task not found | 目标 Task 不存在 |
| `-32001` | Task timeout | 任务执行超时 |
| `-32002` | Task cancelled | 任务被取消 |
| `-32003` | Local agent error | 本地 Agent 执行出错 |
| `-32004` | Capability not supported | 能力不支持 |
| `-32005` | Confirmation denied | 用户拒绝确认 |

## 11. 能力声明规范

本协议中 `capabilities` 是**描述性标签**，不是可被远程调用的工具接口。本地 Agent 自己决定何时调用内部工具、何时请求用户确认；能力标签只供页面展示和按类型过滤。

```json
{
  "type": "chat",
  "name": "coding",
  "description": "编码助手，可读写文件、执行命令、分析代码"
}
```

字段说明：

| 字段 | 必填 | 说明 |
|------|------|------|
| `type` | 是 | `chat`、`resource`、`system` 等，用于页面分类 |
| `name` | 是 | 能力唯一标识，如 `coding`、`oncall`、`office` |
| `description` | 是 | 人类可读的能力描述 |
| `metadata` | 否 | 扩展字段，如 `icon`、`category`、`features` |

`type` 建议取值：

| 值 | 含义 |
|----|------|
| `chat` | 对话型 Agent |
| `resource` | 可提供文件/数据资源 |
| `system` | 系统管理型 Agent |

> 为什么不定义 `inputSchema`？因为外部不是 AI 编排器，不需要替 Agent 决定调用哪个工具。工具调用、参数填充、确认逻辑全部由本地 Agent 自行处理。

## 12. 会话与上下文

- `context_id`：参考 A2A，逻辑上把多个相关 Task / Message 归为一组。同一 `context_id` 下 Agent 可维护长期上下文，用户可基于历史 Task 做追问或 refinement。
- `session_id`：标识一次连续对话，同一 session 内 Agent 可维护上下文。**由 AgentClient（网关）在 `session.create` 时用 `crypto.randomUUID()` 生成**，`task.create` 始终携带，Agent 透传给被控子进程（详见 §6.1）。到达子进程的值必须为合法 UUID。
- `task_id`：标识一次具体任务，一个 session 可包含多个 task。
- `history`：携带历史消息/回合，Agent 可选择性使用。
- `reference_task_ids`：引用之前的 Task，用于基于旧产物做 refinement（参考 A2A `referenceTaskIds`）。

## 13. 安全建议

- **输入校验**：AgentClient 应对 Agent 返回的 JSON 做基本校验
- **危险操作确认**：本地 Agent 自己判断操作是否需要用户确认，并通过 `confirm_required` 输出；AgentClient 不应代替 Agent 做此判断
- **超时控制**：每个 task 应设置超时，防止挂死
- **沙箱限制**：生产环境应对 Agent 可执行命令、访问路径做限制
- **传输安全**：HTTP 适配器建议只绑定 `127.0.0.1`，避免外部访问
- **命令注入防护**：Stdio 适配器避免把用户输入直接拼接到命令行

## 14. 与网关协议的映射

本标准定义的是 AgentClient ↔ 本地 Agent 之间的协议。AgentClient 需要将其映射为网关协议：

| 本地 Agent 消息 | 网关消息 |
|----------------|---------|
| `lifecycle.initialize` / `lifecycle.initialized` | 网关层不感知，由 AgentClient 本地处理 |
| `lifecycle.register` | `system.register` |
| `lifecycle.status` | `system.status` |
| `lifecycle.capabilities_updated` | `system.capabilities_updated`（网关广播 `admin.agent.event` / `admin.agentList`） |
| `stream.chunk` (text/thinking/action/result/confirm_required/prompt_required/block_required) | `$/progress` |
| `stream.artifact` | `admin.task.progress`（type=artifact）或 `admin.task.artifact` |
| `task.respond` | `agent.respond` |
| `task.completed` | `admin.task.progress` (done=true) + 可选 `result` |
| `task.invoke`（§6.6，编排发起） | `agent.task.invoke`（result/error 原样透传回 Agent） |
| `agent.task.result`（网关 → AgentClient） | `task.subtask_result`（AgentClient → Agent 通知） |
| `event.error` | `admin.task.progress` (error) 或 JSON-RPC error |

AgentClient 是这两套协议之间的翻译层。

## 15. 演进路线

| 阶段 | 建议实现 |
|------|---------|
| Demo | `lifecycle.initialize` + `task.create` + `stream.chunk`（text + confirm_required） |
| MVP | 加上 `lifecycle.register` + `lifecycle.status` + `task.cancel` |
| 生产 | 加上心跳、能力更新通知、取消、错误处理、超时、审计、输入校验、typed content |
| 高级 | 加上 Artifact 流式产物、Block Kit 交互、多模态（图片/资源）、文件传输、端到端加密 |
