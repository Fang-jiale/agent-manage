// JSON-RPC 2.0 based communication protocol used between the user page,
// gateway, agent client, and local agents.

export const VERSION = "2.0";

// Standard JSON-RPC 2.0 error codes.
export const ERR_PARSE_ERROR = -32700;
export const ERR_INVALID_REQUEST = -32600;
export const ERR_METHOD_NOT_FOUND = -32601;
export const ERR_INVALID_PARAMS = -32602;
export const ERR_INTERNAL_ERROR = -32603;

// Application specific error codes.
export const ERR_AGENT_NOT_FOUND = -32000;
export const ERR_AGENT_TIMEOUT = -32001;
export const ERR_TASK_CANCELLED = -32002;
export const ERR_LOCAL_AGENT_ERROR = -32003;
export const ERR_UNAUTHORIZED = -32004;
export const ERR_RATE_LIMITED = -32005;

// Method names.
export const METHOD_LIFECYCLE_INITIALIZE = "lifecycle.initialize";
export const METHOD_LIFECYCLE_INITIALIZED = "lifecycle.initialized";
export const METHOD_LIFECYCLE_REGISTER = "lifecycle.register";
export const METHOD_LIFECYCLE_PING = "lifecycle.ping";
export const METHOD_LIFECYCLE_STATUS = "lifecycle.status";
export const METHOD_LIFECYCLE_CAPABILITIES_UPDATED = "lifecycle.capabilities_updated";

export const METHOD_REGISTER = "system.register";
export const METHOD_HEARTBEAT = "system.heartbeat";
export const METHOD_STATUS = "system.status";
export const METHOD_CAPABILITIES_UPDATED = "system.capabilities_updated";

export const METHOD_AGENT_CHAT = "agent.chat";
export const METHOD_AGENT_CANCEL = "agent.cancel";
export const METHOD_AGENT_RESPOND = "agent.respond";

export const METHOD_ADMIN_AGENT_LIST = "admin.agentList";
export const METHOD_ADMIN_AGENT_EVENT = "admin.agent.event";
export const METHOD_ADMIN_PROGRESS = "admin.task.progress";

export const METHOD_PROGRESS = "$/progress";

export const METHOD_TASK_CREATE = "task.create";
export const METHOD_TASK_CANCEL = "task.cancel";
export const METHOD_TASK_RESPOND = "task.respond";

export const METHOD_SESSION_LIST = "session.list";
export const METHOD_SESSION_CREATE = "session.create";
export const METHOD_SESSION_RENAME = "session.rename";
export const METHOD_SESSION_DELETE = "session.delete";
export const METHOD_MESSAGE_LIST = "message.list";

export const METHOD_USER_LIST = "user.list";
export const METHOD_USER_CREATE = "user.create";
export const METHOD_USER_DISABLE = "user.disable";
export const METHOD_USER_RESET_PASSWORD = "user.reset_password";
export const METHOD_USER_CHANGE_PASSWORD = "user.change_password";
export const METHOD_USER_SET_ROLE = "user.set_role";

// 管理后台（均要求 admin 角色）
export const METHOD_AGENT_LIST = "agent.list";
export const METHOD_AGENT_DISCONNECT = "agent.disconnect";
export const METHOD_AGENT_REASSIGN = "agent.reassign";
export const METHOD_ADMIN_OVERVIEW = "admin.overview";

// 品牌目录（admin 维护）与注册审批
export const METHOD_BRAND_LIST = "brand.list";
export const METHOD_BRAND_CREATE = "brand.create";
export const METHOD_BRAND_UPDATE = "brand.update";
export const METHOD_BRAND_DELETE = "brand.delete";
export const METHOD_AGENT_APPROVE = "agent.approve";
export const METHOD_AGENT_REJECT = "agent.reject";

// 连接器（connector）：AgentClient 纯服务模式，agent 实例在页面上分配
export const METHOD_CONNECTOR_HELLO = "connector.hello"; // client → gateway
export const METHOD_CONNECTOR_LIST = "connector.list";   // admin RPC
export const METHOD_CONNECTOR_SYNC = "connector.sync";   // gateway → client，全量目标 agent 集
export const METHOD_AGENT_ASSIGN = "agent.assign";       // 页面分配 agent 实例
export const METHOD_AGENT_REMOVE = "agent.remove";       // 页面移除 agent 实例

// 配对接入：配对码（owner 生成时绑定）→ connector.pair → 审批 → 下发设备密钥
export const METHOD_PAIRING_CREATE = "pairing.create";     // 用户 RPC：生成一次性接入码
export const METHOD_PAIRING_LIST = "pairing.list";         // 用户 RPC：列自己的码
export const METHOD_PAIRING_DELETE = "pairing.delete";     // 用户 RPC：作废自己的码
export const METHOD_CONNECTOR_PAIR = "connector.pair";     // client → gateway（无凭证，凭码）
export const METHOD_CONNECTOR_PENDING_LIST = "connector.pending.list";   // admin RPC
export const METHOD_CONNECTOR_APPROVE = "connector.approve";             // admin RPC：发密钥
export const METHOD_CONNECTOR_REJECT = "connector.reject";               // admin RPC
export const METHOD_CONNECTOR_CREDENTIAL = "connector.credential";       // gateway → client 推送

// 设备密钥（普通用户管理自己的，admin 可管理全员的）
export const METHOD_DEVICE_KEY_CREATE = "device_key.create";
export const METHOD_DEVICE_KEY_LIST = "device_key.list";
export const METHOD_DEVICE_KEY_REVOKE = "device_key.revoke";

// Content item types.
export const CONTENT_TYPE_TEXT = "text";
export const CONTENT_TYPE_IMAGE = "image";
export const CONTENT_TYPE_RESOURCE = "resource";

// Stream chunk types emitted by local agents.
export const CHUNK_TYPE_TEXT = "text";
export const CHUNK_TYPE_THINKING = "thinking";
export const CHUNK_TYPE_ACTION = "action";
export const CHUNK_TYPE_RESULT = "result";
export const CHUNK_TYPE_CONFIRM_REQUIRED = "confirm_required";
export const CHUNK_TYPE_PROMPT_REQUIRED = "prompt_required";
export const CHUNK_TYPE_BLOCK_REQUIRED = "block_required";
export const CHUNK_TYPE_ARTIFACT = "artifact";
// Notification-only (id:null): shim 补发，告诉前端撤销某个待决确认框。
// 触发场景：用户点停止（task_cancelled）、同轮其它操作触发中断（interrupted）、
// ywcoder 子进程异常退出（agent_exited，shim 兜底）。前端收到即关框，幂等。
export const CHUNK_TYPE_CONFIRM_CANCELLED = "confirm_cancelled";
export const CONFIRM_CANCEL_REASON_TASK_CANCELLED = "task_cancelled";
export const CONFIRM_CANCEL_REASON_INTERRUPTED = "interrupted";
export const CONFIRM_CANCEL_REASON_AGENT_EXITED = "agent_exited";

// task.respond 的归一裁决值，前端按钮和 shim 返回都用这三个。
export const RESPOND_DECISION_ALLOW = "allow";
export const RESPOND_DECISION_DENY = "deny";
export const RESPOND_DECISION_CANCEL = "cancel";

// Progress kinds, matching LSP partial result semantics.
export const PROGRESS_KIND_BEGIN = "begin";
export const PROGRESS_KIND_REPORT = "report";
export const PROGRESS_KIND_END = "end";

// Agent status values.
export const AGENT_STATUS_ONLINE = "online";
export const AGENT_STATUS_OFFLINE = "offline";
export const AGENT_STATUS_BUSY = "busy";
export const AGENT_STATUS_IDLE = "idle";

export interface ErrorObject {
  code: number;
  message: string;
  data?: unknown;
}

export interface Message {
  jsonrpc: string;
  id?: string;
  method?: string;
  params?: unknown;
  result?: unknown;
  error?: ErrorObject;
}

export function newRequest(id: string, method: string, params: unknown): Message {
  return { jsonrpc: VERSION, id, method, params };
}

export function newNotification(method: string, params: unknown): Message {
  return { jsonrpc: VERSION, method, params };
}

export function newResponse(id: string, result: unknown): Message {
  return { jsonrpc: VERSION, id, result };
}

export function newErrorResponse(id: string, code: number, message: string, data?: unknown): Message {
  const error: ErrorObject = { code, message };
  if (data !== undefined && data !== null) error.data = data;
  return { jsonrpc: VERSION, id, error };
}

export function decodeParams<T>(msg: Message): T {
  return (msg.params ?? {}) as T;
}

export function isNotification(msg: Message): boolean {
  return msg.id === undefined || msg.id === "";
}

export interface Capability {
  type: string;
  name: string;
  description?: string;
  metadata?: Record<string, unknown>;
}

export interface PlatformInfo {
  os: string;
  arch: string;
  hostname?: string;
}

export interface Resource {
  uri: string;
  mimeType?: string;
  text?: string;
  blob?: string;
}

export interface ContentItem {
  type: string;
  text?: string;
  data?: string;
  mimeType?: string;
  resource?: Resource;
}

export function textContent(text: string): ContentItem[] {
  return [{ type: CONTENT_TYPE_TEXT, text }];
}

export interface ClientServerInfo {
  name: string;
  version: string;
}

export interface InitializeParams {
  protocolVersion: string;
  capabilities?: Record<string, unknown>;
  clientInfo: ClientServerInfo;
}

export interface InitializeResult {
  protocolVersion: string;
  capabilities?: Record<string, unknown>;
  serverInfo: ClientServerInfo;
}

export interface RegisterParams {
  agent_id: string;
  name?: string;
  version?: string;
  description?: string;
  capabilities: Capability[];
  platform?: PlatformInfo;
  brand_id?: string; // 治理模式下必填；name/capabilities 以品牌目录为准被网关覆盖
}

export interface RegisterResult {
  status: string;
  server_time: string;
}

export interface PingParams {
  timestamp: string;
}

export interface PingResult {
  timestamp: string;
}

export interface LifecycleStatusParams {
  status: string;
  task_id?: string;
  message?: string;
}

export interface LifecycleCapabilitiesUpdatedParams {
  capabilities: Capability[];
}

export interface HeartbeatParams {
  agent_id?: string;
  timestamp: string;
}

export interface StatusParams {
  agent_id: string;
  status: string;
  task_id?: string;
  session_id?: string;
  message?: string;
}

export interface CapabilitiesUpdatedParams {
  agent_id: string;
  capabilities: Capability[];
}

export interface AgentInfo {
  id: string;
  owner_id?: string;
  name?: string;
  status: string;
  capabilities: Capability[];
  platform?: PlatformInfo;
  last_heartbeat?: string;
  // 品牌治理：注册时由网关按品牌行覆盖/补充
  brand_id?: string | null;
  brand_name?: string | null;
  logo_url?: string | null;
  approval_status?: string; // approved | pending | rejected（开放模式下恒 approved）
}

export interface AgentListParams {
  agents: AgentInfo[];
}

export interface AgentEventParams {
  event: string;
  agent_id: string;
  timestamp: string;
}

export interface TaskCreateParams {
  agent_id: string;
  task_id: string;
  session_id?: string;
  context_id?: string;
  type: string;
  content: string;
  metadata?: Record<string, unknown>;
}

export interface TaskAcceptResult {
  status: string;
  task_id: string;
}

export interface TaskCancelParams {
  agent_id: string;
  task_id: string;
  session_id?: string;
}

export interface TaskCancelResult {
  task_id: string;
  status: string;
}

export interface TaskRespondParams {
  agent_id: string;
  task_id: string;
  session_id?: string;
  confirm_id?: string;
  prompt_id?: string;
  block_id?: string;
  action_id?: string;
  // 决策对象；旧实现可能仍传 boolean/string，shim 自行兼容归一为 allow/deny/cancel。
  response?: RespondDecision | boolean | string | unknown;
}

// 用户审批结果的归一格式。message 可选，用于填拒绝理由等。
export interface RespondDecision {
  decision: typeof RESPOND_DECISION_ALLOW | typeof RESPOND_DECISION_DENY | typeof RESPOND_DECISION_CANCEL;
  message?: string;
}

// task.respond 的返回值，shim 原样返回，gateway 透传。confirm_id 不存在/已回复/已撤销 → -32000。
// decision 可选——旧 shim 可能传 boolean/string/无法识别的值，normalizeDecision 兜底返回 undefined。
export interface TaskRespondResult {
  task_id: string;
  session_id?: string;
  confirm_id?: string;
  status: "accepted";
  decision?: typeof RESPOND_DECISION_ALLOW | typeof RESPOND_DECISION_DENY | typeof RESPOND_DECISION_CANCEL;
}

export interface TaskCompleteResult {
  task_id: string;
  status: string;
  summary?: string;
}

export interface AgentChatParams {
  task_id: string;
  session_id?: string;
  context_id?: string;
  type: string;
  content: string;
  metadata?: Record<string, unknown>;
  agent_id?: string; // connector 多 agent 托管时由网关注入，client 据此分派
}

export interface AgentCancelParams {
  task_id: string;
  session_id?: string;
  agent_id?: string; // connector 分派用（网关注入）
}

export interface AgentRespondParams {
  task_id: string;
  session_id?: string;
  confirm_id?: string;
  prompt_id?: string;
  block_id?: string;
  action_id?: string;
  response?: unknown;
  agent_id?: string; // connector 分派用（网关注入）
}

export interface ProgressValue {
  kind: string;
  type?: string;
  agent_id: string;
  task_id: string;
  session_id?: string;
  context_id?: string;
  content?: ContentItem[];
  name?: string;
  arguments?: Record<string, unknown>;
  confirm_id?: string;
  prompt_id?: string;
  options?: string[];
  block_id?: string;
  blocks?: unknown;
  percentage?: number;
  done?: boolean;
  error?: string;
  reason?: string;
}

export interface ProgressParams {
  token: string;
  value?: ProgressValue;
}

export interface AdminProgressParams {
  task_id: string;
  type?: string;
  agent_id: string;
  session_id?: string;
  context_id?: string;
  content?: ContentItem[];
  name?: string;
  arguments?: Record<string, unknown>;
  confirm_id?: string;
  prompt_id?: string;
  options?: string[];
  block_id?: string;
  blocks?: unknown;
  percentage?: number;
  done?: boolean;
  error?: string;
  reason?: string;
}

export interface ChatMessage {
  role: string;
  content: string;
}

export interface SessionInfo {
  id: string;
  agent_id: string;
  title: string;
  created_at: number;
  updated_at: number;
  message_count?: number;
}

export interface SessionListParams {
  agent_id?: string;
}

export interface SessionListResult {
  sessions: SessionInfo[];
}

export interface SessionCreateParams {
  id?: string;
  agent_id: string;
  title?: string;
}

export interface SessionRenameParams {
  id: string;
  title: string;
}

export interface SessionDeleteParams {
  id: string;
}

export interface MessageListParams {
  session_id: string;
  limit?: number;
  before?: number;
}

export interface StoredMessage {
  id: string;
  session_id: string;
  agent_id: string;
  role: string;
  content: unknown; // 反序列化后的消息体（{text, attachments?} 或 {chunks}）
  task_id?: string | null;
  created_at: number;
}

export interface MessageListResult {
  messages: StoredMessage[];
  total: number;
}

export interface UserInfo {
  id: string;
  name: string;
  role: string;
  disabled: boolean;
  created_at: number;
  last_login_at: number | null;
}

export interface UserListParams {
  query?: string;
  limit?: number;
  offset?: number;
}

export interface UserListResult {
  users: UserInfo[];
  total: number;
}

export interface UserSetRoleParams {
  id: string;
  role: string; // "admin" | "user"
}

export interface AdminAgentInfo extends AgentInfo {
  first_seen: number;
  last_seen: number;
  online: boolean;
  last_ip?: string | null;
  connector_id?: string | null;
}

export interface AdminAgentListParams {
  owner_id?: string;
  status?: string; // online | busy | offline
  query?: string;
  limit?: number;
  offset?: number;
}

export interface AdminAgentListResult {
  agents: AdminAgentInfo[];
  total: number;
}

export interface AgentDisconnectParams {
  agent_id: string;
}

export interface AgentReassignParams {
  agent_id: string;
  owner_id: string;
}

export interface OverviewResult {
  users_total: number;
  agents_total: number;
  agents_online: number;
  users_connected: number;
  tasks_active: number;
}

export interface DeviceKeyCreateParams {
  name: string;
  owner_id?: string; // 仅 admin 可代他人创建
}

export interface DeviceKeyCreateResult {
  id: string;
  key: string; // 明文仅此一次返回
}

export interface DeviceKeyInfo {
  id: string;
  owner_id: string;
  name: string;
  created_at: number;
  last_used_at: number | null;
  disabled: boolean;
}

export interface DeviceKeyListParams {
  owner_id?: string; // 仅 admin 可查看他人
}

export interface DeviceKeyListResult {
  keys: DeviceKeyInfo[];
}

export interface DeviceKeyRevokeParams {
  id: string;
}

export interface UserCreateParams {
  name: string;
  password: string;
  role?: string; // "admin" | "user"，默认 user
}

export interface UserDisableParams {
  id: string;
  disabled: boolean;
}

export interface UserResetPasswordParams {
  id: string;
  password: string;
}

export interface UserChangePasswordParams {
  old_password: string;
  new_password: string;
}

// ---- 品牌目录与注册审批 ----

export interface BrandInfo {
  id: string;
  name: string;
  description: string;
  logo_url: string | null;
  capabilities: Capability[];
  conn_type: string; // stdio | http | ws：托管实例与本地服务的连接方式
  launch_cmd: string | null; // conn_type=stdio：本地服务启动命令
  endpoint: string | null; // conn_type=http/ws：本地服务地址
  disabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface BrandListResult {
  brands: BrandInfo[];
}

export interface BrandCreateParams {
  name: string;
  description?: string;
  logo_url?: string | null;
  capabilities?: Capability[];
  conn_type?: string; // 默认 stdio
  launch_cmd?: string | null;
  endpoint?: string | null;
}

export interface BrandUpdateParams {
  id: string;
  name: string;
  description?: string;
  logo_url?: string | null;
  capabilities?: Capability[];
  conn_type?: string;
  launch_cmd?: string | null;
  endpoint?: string | null;
  disabled?: boolean;
}

export interface BrandDeleteParams {
  id: string;
}

export interface AgentApprovalParams {
  agent_id: string;
}

// ---- 连接器（connector）----

export interface ConnectorHelloParams {
  connector_id: string;
  platform?: PlatformInfo;
  version?: string;
}

export interface ConnectorInfo {
  id: string;
  owner_id: string;
  platform?: PlatformInfo;
  ip?: string;
  agents: number; // 当前承载的 agent 数
  last_heartbeat: string;
}

export interface ConnectorListResult {
  connectors: ConnectorInfo[];
}

// connector.sync 的全量目标集条目
export interface ConnectorSyncAgent {
  agent_id: string;
  brand_id: string;
  name: string;
  capabilities: Capability[];
  conn_type?: string; // stdio | http | ws（品牌定义），缺省按 launch_cmd 推断
  launch_cmd?: string; // stdio：本地服务启动命令
  endpoint?: string; // http/ws：本地服务地址
}

export interface ConnectorSyncParams {
  agents: ConnectorSyncAgent[];
}

export interface AgentAssignParams {
  connector_id: string;
  brand_id: string;
  name?: string; // 可选自定义后缀/名称，默认 <brand>-<短id>
}

export interface AgentAssignResult {
  agent_id: string;
  status: string;
}

export interface AgentRemoveParams {
  agent_id: string;
}

// ---- 配对接入 ----

export interface PairingCodeCreateParams {
  owner_id?: string; // 仅 admin 可代他人生成，默认自己
  ttl_seconds?: number; // 默认 86400（24h）
}

export interface PairingCodeCreateResult {
  id: string;
  code: string; // 明文仅此一次返回
  owner_id: string;
  expires_at: number;
}

export interface PairingCodeInfo {
  id: string;
  owner_id: string;
  expires_at: number;
  used_at: number | null;
  created_at: number;
}

export interface PairingCodeListResult {
  codes: PairingCodeInfo[];
}

export interface PairingCodeDeleteParams {
  id: string;
}

// client 凭配对码请求接入（无需已有凭证）；批准前连接挂起等待
export interface ConnectorPairParams {
  code: string;
  connector_id: string;
  platform?: PlatformInfo;
  version?: string;
}

export interface ConnectorPairResult {
  status: "pending"; // 已受理，等待管理员审批；批准后推送 connector.credential
}

// 待接入 connector（内存态，网关重启后 client 会重试 pair）
export interface PendingConnectorInfo {
  connector_id: string;
  owner_id: string; // 配对码归属用户，批准后密钥归该用户
  code_id: string;
  platform?: PlatformInfo;
  version?: string;
  ip?: string;
  paired_at: number;
}

export interface ConnectorPendingListResult {
  connectors: PendingConnectorInfo[];
}

export interface ConnectorApproveParams {
  connector_id: string;
}

// 批准后网关推送给 client 的凭证，client 落盘后凭 key 走 connector.hello
export interface ConnectorCredentialParams {
  connector_id: string;
  key: string; // 设备密钥明文，仅此一次
}

export interface LocalAgentRequest {
  task_id?: string;
  session_id?: string;
  context_id?: string;
  type: string;
  content?: string;
  history?: ChatMessage[];
  reference_task_ids?: string[];
  confirm_id?: string;
  prompt_id?: string;
  block_id?: string;
  action_id?: string;
  response?: unknown;
  metadata?: Record<string, unknown>;
}

export interface LocalAgentChunk {
  type: string;
  task_id?: string;
  session_id?: string;
  context_id?: string;
  content?: ContentItem[];
  name?: string;
  arguments?: Record<string, unknown>;
  confirm_id?: string;
  prompt_id?: string;
  options?: string[];
  block_id?: string;
  blocks?: unknown;
  percentage?: number;
  done?: boolean;
  error?: string;
  reason?: string;
}

export function rfc3339Now(): string {
  return new Date().toISOString();
}
