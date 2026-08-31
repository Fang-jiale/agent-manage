// 网关共享类型与纯函数辅助：连接/任务态、编排限额、chunk 处理。
// 从 gateway.ts 原样迁出（行为零变化），被 hub 与各 handler 模块共用。
import { WebSocket } from "ws";
import * as proto from "../protocol.ts";

export interface AgentConn {
  id: string;
  ownerID: string;
  name: string;
  ws: WebSocket;
  capabilities: proto.Capability[];
  platform?: proto.PlatformInfo;
  status: string;
  lastHeartbeat: number;
  alive: boolean;
  deviceKeyID?: string; // 设备密钥连接记录 key id，吊销时按此踢线
  lastDbTouch?: number; // agents 表状态落库节流用
  lastRegistryTouch?: number; // 注册表刷新节流用
  ip?: string; // AgentClient 连接远端 IP（X-Forwarded-For 优先，否则 socket 远端）
  brandID?: string; // 品牌目录 id（治理模式）
  connectorID?: string; // 承载该 agent 的 connector（页面分配的实例才有）
  approval?: string; // approved | pending；pending 可用连接但不可接任务
  pairing?: boolean; // 无凭证的配对连接（?pair=1），只允许 connector.pair
}

// 待接入 connector（内存态）：pair 受理后挂起，等管理员批准后下发设备密钥
export interface PendingPair {
  connectorID: string;
  ownerID: string; // 配对码归属用户，批准后密钥归该用户
  codeID: string;
  conn: AgentConn;
  platform?: proto.PlatformInfo;
  version?: string;
  ip?: string;
  pairedAt: number;
}

export interface UserConn {
  ws: WebSocket;
  userID: string;
  lastHeartbeat: number;
  alive: boolean;
  isAdmin: boolean; // WS 升级时缓存的角色；角色变更通过 kickUser 强制重连刷新
  ownOnly: boolean; // 对话页连接（?scope=own）：即使 admin 也只推送/通知自己名下的 agent
}

export interface TaskState {
  agentID: string;
  ownerID: string;
  sessionID: string;
  timer: NodeJS.Timeout;
  createdAt: number;
  groupID?: string;       // 群聊任务：归属群
  parentTaskID?: string;  // 管理者编排：发起 invoke 的父任务
  invokerAgentID?: string; // 管理者编排：管理者 agent（子任务结束时回投结果）
  depth: number;          // 0 = 用户直发，1..MAX_ORCHESTRATION_DEPTH = 编排子/孙任务
  subtasksDispatched?: number; // 本任务已派发子任务累计（预算制：连同已完成的也计数）
  contextPolicy?: "final_only" | "full"; // 编排子任务结果回投策略（缺省 final_only）
  invocationID?: string;  // 编排子任务的幂等键（记录到父任务的 invocation 记录）
  threadSessionID?: string; // 续聊线程：目标 agent 所见的子会话 id（落库归因仍走 sessionID）
  invocations?: Map<string, InvocationRecord>; // 父任务名下按 invocation_id 分组的子任务记录（幂等/对账）
}

// 单个 invocation_id（管理者一次逻辑调用）名下的子任务集合。
// 挂在父任务的 TaskState 上，父任务结束随之消亡；children 上限防长任务膨胀。
// collect 策略：first/quorum 条件满足时收割（取消）其余运行中的兄弟任务。
export interface InvocationRecord {
  invocationID: string;
  collect: "all" | "first" | { quorum: number };
  collected: boolean; // 收割只触发一次
  children: Array<{
    taskID: string;
    target: string;
    done: boolean;
    ok: boolean;
    lastResult?: proto.AgentTaskResultParams; // 终态结果留存：duplicate invoke 重发用
  }>;
}

// 单个父任务同时运行的编排子任务上限（防管理者连发把 worker 打满）
export const MAX_PARENT_SUBTASKS_RUNNING = 4;
// 单父任务子任务总数预算（含已完成：预算制取代旧的单层硬限，多级编排仍受总账约束）
export const MAX_PARENT_SUBTASKS_TOTAL = 16;
// 编排深度上限：depth 0（用户任务）→1→2→3，第 4 层拒绝（防递归硬限）
export const MAX_ORCHESTRATION_DEPTH = 3;
// 单父任务 invocation 记录条数上限（FIFO 淘汰；每条最多 MAX_GROUP_FANOUT 个子任务）
export const MAX_PARENT_INVOCATIONS = 64;
// per-invoke 超时覆盖的夹取范围
export const INVOKE_TIMEOUT_MIN_MS = 1_000;
export const INVOKE_TIMEOUT_MAX_MS = 3_600_000;

// 群消息 fan-out 的单群目标 agent 上限
export const MAX_GROUP_FANOUT = 8;

export interface PendingEntry {
  user: UserConn;
  timer: NodeJS.Timeout; // agent 不应答时兜底，防泄漏
  taskID?: string; // agent.chat 转发携带：agent 以错误响应拒绝时用于清理任务条目
}

export interface TaskBuffer {
  ownerID: string;
  agentID: string;
  sessionID: string;
  chunks: proto.LocalAgentChunk[];
  bytes: number;    // 已缓冲体积，配合 MAX_TASK_BUFFER_BYTES 防长任务撑爆内存
  truncated: boolean;
}

// 单任务落库缓冲上限：超出部分丢弃并在最终消息里标注截断
export const MAX_TASK_BUFFER_BYTES = 4 * 1024 * 1024;

// WS 发送背压水位：软水位丢通知（慢消费），硬水位断开（对端停摆，防 OOM）
export const SEND_BUFFER_SOFT = 8 * 1024 * 1024;
export const SEND_BUFFER_HARD = 32 * 1024 * 1024;

// 交互 chunk（confirm/prompt/block）在任务缓冲里的应答/撤销标记，随任务落库
export interface BufferedInteractionChunk {
  type?: string;
  confirm_id?: string;
  prompt_id?: string;
  block_id?: string;
  answered?: boolean;
  answer?: unknown;
  cancelled?: boolean;
  reason?: string;
}

export function isInteractionChunk(c: BufferedInteractionChunk): boolean {
  return c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED
    || c.type === proto.CHUNK_TYPE_PROMPT_REQUIRED
    || c.type === proto.CHUNK_TYPE_BLOCK_REQUIRED;
}

// respond 只带命中的 id；id 缺省（旧 client）时退化为同类型首个待决 chunk
export function matchesInteractionChunk(
  c: BufferedInteractionChunk, confirmID: string, promptID: string, blockID: string,
): boolean {
  if (c.type === proto.CHUNK_TYPE_CONFIRM_REQUIRED) return confirmID === "" || c.confirm_id === confirmID;
  if (c.type === proto.CHUNK_TYPE_PROMPT_REQUIRED) return promptID === "" || c.prompt_id === promptID;
  return blockID === "" || c.block_id === blockID;
}

// chunk 序列里的文本内容（content[].text 拼接）
export function textOfChunk(c: { type?: string; content?: proto.ContentItem[] }): string {
  return (c.content ?? []).map((ci) => ci.text ?? "").join("");
}

// final_only 策略：取尾部连续 text chunk 的终态文本（中间的 thinking/工具过程丢弃），
// artifact（产出）chunk 一律保留；无尾部 text 时回退全部 text 拼接。
export function finalOnlyChunks(chunks: proto.LocalAgentChunk[]): proto.LocalAgentChunk[] {
  const tail: proto.LocalAgentChunk[] = [];
  for (let i = chunks.length - 1; i >= 0; i--) {
    const c = chunks[i];
    if (c.type !== proto.CHUNK_TYPE_TEXT) break;
    tail.unshift(c);
  }
  const finalText = tail.map(textOfChunk).join("").trim();
  const artifacts = chunks.filter((c) => c.type === proto.CHUNK_TYPE_ARTIFACT);
  if (finalText === "" && artifacts.length === 0) {
    const all = chunks.filter((c) => c.type === proto.CHUNK_TYPE_TEXT).map(textOfChunk).join("").trim();
    return all !== "" ? [{ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(all) }] : [];
  }
  return [
    ...(finalText !== "" ? [{ type: proto.CHUNK_TYPE_TEXT, content: proto.textContent(finalText) }] : []),
    ...artifacts,
  ];
}

