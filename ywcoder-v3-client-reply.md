# YwCoder 适配本地 Agent 接口 v3：Client 答复

对应《ywcoder-v3-client-confirmation.md》逐项答复。C1、C3、C6 接受并定稿；C2、C4、C7 为页面/联调约定确认；C5 知悉无异议。

## C1. Capability 两级作用域 — 接受，定稿

- 接受 `lifecycle.capabilities_updated.params.session_id` 为可选字段，语义按建议：
  - 不带 `session_id`：Agent 全局能力全量快照；
  - 带 `session_id`：该 session/workdir 的命令与技能全量快照。
- 合并规则：页面同时持有全局与当前 session 两层快照，同 `type/name` 冲突时 session 层优先。
- 全量替换仅作用于本次消息对应的层级；session 更新不覆盖全局快照。
- session 关闭时，Client 清理该 session 层的 capability 快照。
- 缓存键：全局层 `agent_id`，session 层 `(agent_id, session_id)`。
- 网关透传：`system.capabilities_updated` 广播帧携带 `session_id`（存在时）。

兼容性：`session_id` 可选，不带该字段的现有 Agent 行为完全不变。

## C2. 权限模式选项 — 接受，页面已动态渲染

- 页面直接使用 `metadata.args[].options` 渲染，无硬编码 `auto/full_auto`，无需输入别名兼容。
- `metadata.current` 回写原生值（`default` / `acceptEdits` / `bypassPermissions`）。
- `bypassPermissions` 仅在 shim 启动显式允许时上报，页面按 options 原样呈现。

## C3. 本 Agent 实例 ID — 接受，定稿

- 字段：`lifecycle.initialize.params.agentInfo.agent_id`。
- 数据来源：网关认可的真实实例 ID。connector 模式下为网关注册分配的 `agent_id`（非 shim 自报名）；本地模式为 AgentClient 分配的实例 ID。
- 下发时机：AgentClient 创建 adapter、发送 `lifecycle.initialize` 时随 params 下发，先于任何任务。
- shim 在 `lifecycle.register` 中自报的名称仅作展示参考，不参与受信判断。

## C4. Workdir 切换语义 — 接受

- 页面在 session 首个任务发出后锁定目录选择。
- 用户切换目录时，页面自动创建新 session，不向已绑定 session 发送不同 workdir。
- 如仍收到不同 workdir，以 YwCoder 返回的 `-32602` 为准，页面展示错误，不静默切换上下文。

## C5. Workdir 信任边界 — 知悉，无异议

按文中约定执行：Client/网关保证 `metadata.workdir` 来源、shim 校验绝对路径/存在性/目录类型并以 `realpath` 规范化、非法目录返回 `-32602`。暂不增加 allowlist。

## C6. 父子任务取消 — 接受，定稿

- 级联取消由网关负责：父任务取消（用户取消或任务超时）时，按 `parent_task_id` 级联取消所有未完成子任务，与现有群组任务取消语义一致。
- AgentClient 通知 shim 的消息：沿用现有 `task.cancel` 通知（JSON-RPC notification，params 含 `task_id`、`session_id`），每个被级联的子任务单独下发一条；shim 收到后结束对子任务结果的等待、中止本地委派工具调用并完成父任务收尾。
- 已完成子任务不受影响；迟到或重复的 `task.subtask_result` 由网关幂等忽略。

## C7. 命令和技能发现时机 — 接受

- 按建议流程执行：register 上报全局能力 → session 建立并确定 workdir → 子进程 initialize → 带 `session_id` 的 capability 更新上报项目命令与技能。
- 子进程 ready 前页面显示"正在加载项目技能"。
- `/model` 下拉能力延迟上报（首个子进程 initialize 返回统一模型列表后，以不带 `session_id` 的全局 capability 更新补充），页面按 capability 到达动态渲染，接受分阶段加载。

## Agent 全局设置补充约定 — 确认一致

确认模型与权限模式按 Agent 实例全局维护，不按 session 隔离：

- 任一 session 切换模型或权限即更新该 Agent 全局设置；其他 session 不打断执行中任务，在各自下一任务前应用最新设置。
- 切换目录、新建 session、恢复 session 均自动继承当前全局模型与权限。
- `metadata.current` 通过不带 `session_id` 的全局 capability 更新发布。

Client 文档将同步把模型/权限作用域表述从 session 级调整为实例全局级。

## Client 侧配套改动清单

| 项 | 改动 | 所在层 |
|---|---|---|
| C1 | 协议增加可选 `session_id`；网关广播透传；页面两层合并展示（session 层优先） | protocol / gateway / 页面 |
| C3 | `lifecycle.initialize` params 增加 `agentInfo.agent_id`（实例 ID 注入 adapter） | client / adapters |
| C6 | 单 Agent 任务取消与超时路径按 `parent_task_id` 级联取消（群组路径已具备） | gateway |
| C4 | 首任务后锁定目录；换目录自动新建 session | 页面 |
| C7 | "正在加载项目技能"加载态 | 页面 |
| 全局设置 | 文档作用域表述由 session 级改为实例全局级 | docs |

兼容性结论：以上改动均向后兼容。C1/C3 为可选/新增字段（旧 Agent 忽略即可），C6 为网关内部行为补齐，C2/C5/C7 无协议变化；C4 是页面 UX 变化，不影响既有协议。
