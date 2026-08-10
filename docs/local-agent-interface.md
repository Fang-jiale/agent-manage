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
- 双向 JSON 消息

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
| `task.*` | 任务创建、回复、取消、完成 |
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

### 6.2 用户回复

当 Agent 输出 `confirm_required`、`prompt_required` 或 `block_required` 后，AgentClient 将用户回复转发给 Agent。`response` 可以是字符串（确认/选择），也可以是 block 表单结果对象。

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
    "response": "确认"
  }
}
```

### 6.3 取消任务

```json
// AgentClient → Agent
{
  "jsonrpc": "2.0",
  "id": "req-003",
  "method": "task.cancel",
  "params": {
    "task_id": "task-001"
  }
}
```

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
    "level": "dangerous",
    "timeout": 60
  }
}
```

`level` 枚举：`info`、`warning`、`dangerous`。

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
- `session_id`：标识一次连续对话，同一 session 内 Agent 可维护上下文。
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
| `event.error` | `admin.task.progress` (error) 或 JSON-RPC error |

AgentClient 是这两套协议之间的翻译层。

## 15. 演进路线

| 阶段 | 建议实现 |
|------|---------|
| Demo | `lifecycle.initialize` + `task.create` + `stream.chunk`（text + confirm_required） |
| MVP | 加上 `lifecycle.register` + `lifecycle.status` + `task.cancel` |
| 生产 | 加上心跳、能力更新通知、取消、错误处理、超时、审计、输入校验、typed content |
| 高级 | 加上 Artifact 流式产物、Block Kit 交互、多模态（图片/资源）、文件传输、端到端加密 |
