# task.create 拒绝响应丢失修复方案（ack 透传改造）

> 状态：✅ 已实现（2026-08-26，含 §6 测试与真实链路复测；ack 时限取 30s；协议文档已补「应答时限与隐式接受」一节）
> 关联：ywmatrix-shim v3 真实链路联测（2026-08-25）发现的链路缺陷；联测清单 §5.7 预期「坏 workdir → task.create 直接 -32602」当前不成立。
> 涉及方：agent-manage（本仓库，网关 + AgentClient + 适配器）；shim 侧无需改动（行为已正确）。

## 1. 问题现象

用户在页面给会话设置了一个不存在的工作目录（或绑定后换目录），发消息后：

- 页面看到 `task.create` 返回 `accepted`，任务开始转圈；
- 之后**永远没有任何终态**——不报错、不结束，挂到 30 分钟任务超时才被兜底清理。

凡 shim 在 `task.create` 阶段以 JSON-RPC 错误响应拒绝的场景全部中招：

| 场景 | shim 返回 |
|---|---|
| `metadata.workdir` 目录不存在/不可访问 | `-32602` |
| 同会话换目录（违反首任务绑定，C4） | `-32602` |
| 群上下文不一致（单聊后带 group / 群 id 变更） | `-32602` |
| `session_id` 非法（非 UUID 等） | `-32602` |

shim 行为本身正确——绕过 AgentClient 直接向 shim stdin 发同样请求，立即收到 `-32602` 错误响应。错误是在 AgentClient 内部丢的。shim 的 mock 回归一直 PASS 是因为 mock 直接断言协议响应，没覆盖真实 AgentClient 这条路径。

## 2. 根因链（三层叠加）

```text
网关 ──agent.chat──► AgentClient ──task.create──► shim
                         │                          │ 校验失败
                         │ ① 立即回 accepted          │ 回 {id, error:{-32602}}
                         │   （还没问 shim）    ◄─────│
                         │ ② stdio/ws 适配器把该响应
                         │   丢进 default 分支忽略
                         ▼
                    错误消失，任务悬挂
```

1. **乐观 ack**：`client.ts handleChat` 收到 `agent.chat` 后先把 `{status:"accepted"}` 回给网关（client.ts:295-298），再联系本地 Agent——页面必然显示"已接受"。
2. **适配器丢响应**：`StdioAdapter.handleAgentMessage` 的 switch 只处理通知类消息（`stream.chunk`、`lifecycle.*`、`event.error`…），带 `id` 的 JSON-RPC **响应**落入 `default:` 被忽略（stdio.ts；ws.ts:160 同样，注释即「响应忽略」）。HTTP 适配器无此问题：`POST /tasks` 非 200 会 throw（http.ts:55）。
3. **网关无清理**：`handleTaskCreate` 转发前已 `trackTask`（gateway.ts:2405），agent 侧此后无任何消息，任务条目只能等 30min 超时。

## 3. 目标与非目标

**目标**

- shim 的 `task.create` 错误响应原样传回页面：用户 `task.create` 直接收到 `-32602 + 原因文案`，不再悬挂；
- 不改变正常任务的协议语义与可感知时延；
- 兼容存量不回 ack 的 stdio agent（见 §5.3）。

**非目标**

- 不改协议文档：`local-agent-interface` 本就规定 task.create 有 ack/error 响应，本次是让实现跟上协议；
- 不改 shim 行为；
- 不处理运行中任务的失败路径（`event.error` → 终态 chunk 已有兜底，不在本期）。

## 4. 方案详设

核心：**AgentClient 消费本地 Agent 的 task.create 应答，把 ack 时机从「立即」改为「本地应答之后」。**

### 4.1 适配器层（stdio.ts / ws.ts）

新增请求-响应关联：

```ts
// 适配器内新增
private pendingAcks = new Map<string, {
  resolve: (result: unknown) => void;
  reject: (err: { code: number; message: string }) => void;
  timer: NodeJS.Timeout;
}>();
```

- `send()` 写出请求时登记 `pendingAcks[id]`（id 即 task_id），等待时限 **10s**；
- `handleAgentMessage` 的 `default:` 分支改为：若 `msg.id` 命中 `pendingAcks`——`msg.error` → reject（透传 code/message）；`msg.result` → resolve；
- `send()` 在返回 chunk queue 之前 `await` 这个 ack：
  - 成功 → 返回 queue（现状不变）；
  - 拒绝/超时 → throw，由 client 统一处理。

**隐式 ack（兼容关键）**：任务的首个 `stream.chunk` 到达时，若该 task 的 ack 仍在等待，视为隐式接受——`resolve`。理由：存量 agent（如 `examples/stdio-agent.py`）不回 ack 但会立即开始推流，流本身就是接受证据；只有「既不应答也不出流」才判定异常。这样旧 agent 零改动兼容，新 agent 的显式拒绝仍能秒级传回。

- ws.ts 同构修改；
- http.ts 不改（非 200 已 throw，语义天然一致）。

### 4.2 client.ts（handleChat）

```text
现状：收到 agent.chat → 回 accepted → adapter.send() → 转发 chunk 流
改为：收到 agent.chat → adapter.send()（内含等待本地 ack）
        ├─ 成功 → 回网关 {status:"accepted"} → 登记 TaskRegistry → 转发 chunk 流
        └─ 拒绝 → 回网关 error 响应（透传 code/message）→ 结束
```

- 拒绝路径**不登记 TaskRegistry、不发 busy 状态**（任务从未开始）；
- 现有 catch 路径（`adapter.send()` throw → 发终态 error chunk）保留，用于 ack 成功后的运行期失败；
- `task.respond` 走同一 ack 机制（带 id 时）：成功透传 result（现状 client.ts:393-400 已有），错误按 §6.2.2 约定处理——`-32000`（confirm 已撤销/已回复）属正常竞态，仅记 debug 日志，不上报页面；通知形式（无 id）的 respond 无 ack，跳过等待。

### 4.3 gateway.ts（被拒任务清理）

`trackTask` 先于转发执行，agent 拒绝时需要清理，否则任务条目挂到超时：

- `trackPendingRequest` 的条目附带 `task_id`（`handleTaskCreate` 调用时传入）；
- agent 响应为 **error** 时（`handleAgentMessage` default 分支，forwardToPendingUser 之前/之中）：`untrackTask(task_id)` + `observeTaskEnd(task_id, "failed")`；
- 成功响应不动任务状态（任务生命周期仍由 chunk 流的 done 终态驱动）。

用户消息已持久化（`persistUserMessage`）不受影响——拒绝的任务在用户消息历史上保留，符合审计预期。

### 4.4 修复后三种场景时序

| shim 行为 | 页面看到 |
|---|---|
| 正常 ack | `task.create` 返回 accepted（比现状晚毫秒级，shim 校验同步，无感） |
| 错误响应（-32602 等） | `task.create` 直接返回错误 + 原因文案，任务不创建 |
| 既无 ack 也无流（卡死） | 10s 后 `task.create` 返回本地 agent 无应答错误 |

## 5. 边界与兼容性

### 5.1 存量 stdio agent

不回 task.create ack 的旧 agent 由「隐式 ack」（§4.1）兼容：首 chunk 即视为接受。一个 task 全程无 chunk 又无 ack 的旧 agent 会在 10s 被判失败——按协议这本就是非法实现（任务必须产出终态），可接受。

### 5.2 群组与编排路径

群任务 fan-out、管理者编排子任务（`agent.task.invoke`）最终都复用同一条 `agent.chat` 下发链路，自动继承本修复，无需单独处理。

### 5.3 行为变化通告

修复后页面会在上述拒绝场景收到 `task.create` 的 **error 响应**（此前只会成功）。页面已处理 error 路径（agent not found / 越权等同样是 error 响应），无需改动，但建议同步 shim 侧与前端知悉行为变化。

## 6. 测试计划

**单测**

- `test/ws-adapter.test.ts` / stdio 适配器：fake agent 分别回 ①正常 ack ②`-32602` ③无响应——断言 send() resolve / reject(code,message) / 10s 超时 reject；首 chunk 隐式 ack 路径；
- gateway 层：agent 回 error 响应后 `tasks` 表清空、用户收到原样 error。

**集成回归**

- `test/gateway.test.ts` 增加：假 stdio agent 对特定 content 回 `-32602`，断言用户 `task.create` 收到 `-32602`、无悬挂任务；正常 agent 全链路不回归；`npm test` 全绿。

**真实链路复测**

- 重跑 2026-08-25 联测脚本（ywc-real + shim）：7a（坏 workdir）、7c（同会话换目录）应从「悬挂」变为「直接 -32602」；项 1/8 不回归。

## 7. 影响文件清单

| 文件 | 改动 |
|---|---|
| `src/adapters/stdio.ts` | pendingAcks 关联、default 分支响应路由、send() 等 ack、隐式 ack |
| `src/adapters/ws.ts` | 同构修改 |
| `src/client.ts` | handleChat ack 后移 + 拒绝路径；respond 错误分级处理 |
| `src/gateway.ts` | pendingRequest 附带 task_id；error 响应清理任务 |
| `test/ws-adapter.test.ts` / `test/gateway.test.ts` | 上述用例 |
