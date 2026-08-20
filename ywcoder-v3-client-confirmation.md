# YwCoder 适配本地 Agent 接口 v3：Client 待确认事项

请协助确认以下协议与交互约定。其中 C1、C3、C6 需要正式定稿；C2、C4、C7 主要确认页面行为和联调约定。C5 已按当前内网信任模型确定，无需新增协议字段。

## C1. Capability 两级作用域

建议将 `lifecycle.capabilities_updated.params.session_id` 定义为可选字段：

- 不带 `session_id`：表示 Agent 全局能力的全量快照。
- 带 `session_id`：表示指定 session/workdir 的命令和技能全量快照。
- 页面展示时合并全局快照与当前 session 快照；同 `type/name` 的能力由 session 层优先。
- “全量替换”仅作用于本次消息对应的层级，session 更新不能覆盖 Agent 全局快照。
- session 关闭时，Client 清理该 session 对应的 capability 快照。

示例：

```jsonc
// Agent 全局能力
{
  "jsonrpc": "2.0",
  "method": "lifecycle.capabilities_updated",
  "params": {
    "capabilities": []
  }
}

// 指定 session/workdir 的能力
{
  "jsonrpc": "2.0",
  "method": "lifecycle.capabilities_updated",
  "params": {
    "session_id": "session-001",
    "capabilities": []
  }
}
```

建议 Client 分别以 `agent_id` 和 `(agent_id, session_id)` 为缓存键。

**请确认：** 是否接受可选 `session_id`、上述合并优先级、分层全量替换和 session 关闭清理规则。

## C2. 权限模式选项

权限选项由 YwCoder capability 动态上报，使用 YwCoder 原生值：

- `default`
- `acceptEdits`
- `bypassPermissions`

页面应直接使用 `metadata.args[].options` 渲染，不硬编码 `auto/full_auto`。只有 shim 启动时显式允许，才会上报和接受 `bypassPermissions`。

如果 Client 现有实现已经硬编码示例值，请告知；YwCoder 可以临时兼容输入别名：

- `auto → acceptEdits`
- `full_auto → bypassPermissions`

capability 的 `metadata.current` 始终回写原生值。

**请确认：** 页面是否能够完全使用 Agent 动态上报的 options。

## C3. 本 Agent 实例 ID

群管理者编排需要进行以下受信判断：

```text
metadata.group.manager_agent_id === 本 Agent 实例 ID
```

建议 AgentClient 在初始化时下发网关认可的真实实例 ID：

```text
lifecycle.initialize.params.agentInfo.agent_id
```

不能使用 shim 在 `lifecycle.register` 中自报的名称代替网关分配的实例身份。

**请确认：** 字段名称、数据来源和下发时机。

## C4. Workdir 切换语义

首版建议：

- session 的首个任务绑定 workdir。
- 同一 session 后续不能修改 workdir。
- 用户切换目录时，页面创建新 session。
- 如果仍向已绑定 session 发送不同 workdir，Agent 返回 JSON-RPC `-32602`，不静默切换上下文或 transcript。

**请确认：** 页面是否可以在首任务后锁定目录，或者在用户切换目录时自动创建新 session。

## C5. Workdir 信任边界（已确定）

当前使用统一内网网关，`metadata.workdir` 来自用户在 Client 中设置并由网关持久化的工作目录，因此不增加 workspace roots 或 `--allowed-workdir-root` 配置。

职责约定：

- Client/网关保证 `metadata.workdir` 只来自用户的目录选择或会话设置。
- shim 校验绝对路径、存在性和目录类型，并使用 `realpath` 规范化。
- 非法、已删除或不可访问的目录返回 JSON-RPC `-32602`。

如果未来允许第三方 API、跨租户网关或其他非受信来源直接写入 workdir，再增加可选目录 allowlist。

此项仅同步约定，无需新增协议字段或配置项。

## C6. 父子任务取消

群管理者通过 `task.invoke` 派发子任务后，如果父任务被取消，建议：

- 网关根据 `parent_task_id` 级联取消所有未完成子任务。
- AgentClient 通知 shim 结束对子任务结果的等待。
- shim 中止本地委派工具调用并完成父任务收尾。
- 已完成子任务不受影响；迟到或重复的 `task.subtask_result` 幂等忽略。

**请确认：** 是否由网关负责级联取消，以及 AgentClient 将通过哪一种消息通知 shim 完成收尾。

## C7. 命令和技能发现时机

建议采用以下流程：

1. `lifecycle.register` 先上报稳定的 Agent 全局能力。
2. 用户创建或打开 session，并确定 workdir。
3. 对应 YwCoder 子进程完成 initialize。
4. Agent 通过带 `session_id` 的 `lifecycle.capabilities_updated` 上报该 workdir 的命令和技能。
5. 子进程 ready 前，页面显示“正在加载项目技能”。

启动时如果还没有真实模型 options，暂不上报 `/model` 下拉能力；首个子进程 initialize 返回统一网关模型列表后，再通过不带 `session_id` 的全局 capability 更新补充。

**请确认：** 页面是否接受该分阶段加载流程及加载状态。

## Agent 全局设置补充约定

当前内网产品中，模型和权限模式按 Agent 实例全局维护，不按 session 隔离：

- 从任一 session 切换模型或权限，更新该 Agent 的全局设置。
- 其他 session 不打断正在执行的任务，在各自下一任务前应用最新设置。
- 切换目录、新建 session 和恢复 session 都自动继承当前全局模型与权限。
- 模型/权限的 `metadata.current` 通过不带 `session_id` 的全局 capability 更新发布。

请一并确认该全局作用域与 Client 当前产品行为一致。

## 确认优先级

### 必须正式定稿

- C1：Capability 两级作用域。
- C3：本 Agent 实例 ID 的下发。
- C6：父任务取消后的子任务级联取消。

### 页面和联调确认

- C2：权限 options 动态渲染。
- C4：首任务后锁定 workdir/换目录新建 session。
- C7：项目命令和技能的分阶段加载。
- Agent 全局模型/权限作用域。

### 已确定，仅同步

- C5：直接使用用户设定的工作目录，不增加 allowed-root 配置。
