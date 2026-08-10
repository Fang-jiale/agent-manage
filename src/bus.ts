// Redis 总线：多实例网关之间的 agent 注册表与消息扇出。
// 未配置 REDIS_URL 时网关以单机模式运行（不创建 Bus）。
//
// 注册表：
//   ywm:agent:{id}   — JSON（含 instance_id），带 TTL，随心跳刷新
//   ywm:agents       — agent id 集合（枚举用，过期 id 懒清理）
// 频道：
//   ywm:i:{instance} — 点对点：发往本实例持有的 agent 连接
//   ywm:users        — 广播：用户通知 / pending 响应 / agent 列表变更

import { createClient } from "redis";
import * as proto from "./protocol.ts";
import { logger } from "./util.ts";

export interface RegisteredAgent {
  id: string;
  owner_id: string;
  name: string;
  status: string;
  capabilities: proto.Capability[];
  platform?: proto.PlatformInfo;
  instance_id: string;
  last_heartbeat: number;
}

export type BusEnvelope =
  | { kind: "agent_msg"; agent_id: string; msg: proto.Message }
  | { kind: "user_msg"; owner_id: string; msg: proto.Message; src?: string }
  | { kind: "pending"; req_id: string; msg: proto.Message }
  | { kind: "agents_changed"; src?: string };

export interface BusHandlers {
  onAgentMessage(agentID: string, msg: proto.Message): void;
  onUserMessage(ownerID: string, msg: proto.Message): void;
  onPendingResponse(reqID: string, msg: proto.Message): void;
  onAgentsChanged(): void;
}

type RedisClient = ReturnType<typeof createClient>;

export class Bus {
  readonly instanceID: string;
  private pub: RedisClient;
  private sub: RedisClient;
  private handlers: BusHandlers;
  private registryTtlSec: number;
  // 频道/键前缀：默认 ywm；测试或多环境共用 Redis 时用独立前缀隔离
  private prefix: string;

  constructor(url: string, instanceID: string, registryTtlMs: number, handlers: BusHandlers, prefix = "ywm") {
    this.instanceID = instanceID;
    this.registryTtlSec = Math.max(10, Math.ceil(registryTtlMs / 1000));
    this.handlers = handlers;
    this.prefix = prefix;
    this.pub = createClient({ url });
    this.sub = createClient({ url });
  }

  private keyAgents(): string { return `${this.prefix}:agents`; }
  private keyAgent(id: string): string { return `${this.prefix}:agent:${id}`; }
  private chInstance(id: string): string { return `${this.prefix}:i:${id}`; }
  private chUsers(): string { return `${this.prefix}:users`; }

  async start(): Promise<void> {
    this.sub.on("error", (e: unknown) => logger.error("redis sub error", { error: String(e) }));
    this.pub.on("error", (e: unknown) => logger.error("redis pub error", { error: String(e) }));
    await this.pub.connect();
    await this.sub.connect();
    await this.sub.subscribe(this.chInstance(this.instanceID), (raw: string) => this.dispatch(raw));
    await this.sub.subscribe(this.chUsers(), (raw: string) => this.dispatch(raw));
  }

  async stop(): Promise<void> {
    await this.sub.unsubscribe().catch(() => {});
    await this.sub.destroy();
    await this.pub.destroy();
  }

  async ping(): Promise<boolean> {
    try {
      await this.pub.ping();
      return true;
    } catch {
      return false;
    }
  }

  private dispatch(raw: string): void {
    let env: BusEnvelope;
    try {
      env = JSON.parse(raw) as BusEnvelope;
    } catch {
      return;
    }
    // 本实例发出的广播已在本地处理过（forwardToUsers / registerAgent 的本地分支），跳过回声
    if ((env.kind === "user_msg" || env.kind === "agents_changed") && env.src === this.instanceID) {
      return;
    }
    switch (env.kind) {
      case "agent_msg": this.handlers.onAgentMessage(env.agent_id, env.msg); break;
      case "user_msg": this.handlers.onUserMessage(env.owner_id, env.msg); break;
      case "pending": this.handlers.onPendingResponse(env.req_id, env.msg); break;
      case "agents_changed": this.handlers.onAgentsChanged(); break;
    }
  }

  // ---- 注册表 ----

  async registerAgent(a: RegisteredAgent): Promise<void> {
    await this.pub
      .multi()
      .set(this.keyAgent(a.id), JSON.stringify(a), { EX: this.registryTtlSec })
      .sAdd(this.keyAgents(), a.id)
      .exec();
    await this.publishAgentsChanged();
  }

  // 心跳/状态刷新：重写 value 并重置 TTL
  async refreshAgent(a: RegisteredAgent): Promise<void> {
    await this.pub.set(this.keyAgent(a.id), JSON.stringify(a), { EX: this.registryTtlSec });
  }

  async unregisterAgent(agentID: string): Promise<void> {
    await this.pub.multi().del(this.keyAgent(agentID)).sRem(this.keyAgents(), agentID).exec();
    await this.publishAgentsChanged();
  }

  async getAgent(agentID: string): Promise<RegisteredAgent | undefined> {
    const raw = await this.pub.get(this.keyAgent(agentID));
    if (!raw) return undefined;
    try {
      return JSON.parse(raw) as RegisteredAgent;
    } catch {
      return undefined;
    }
  }

  async listAgents(): Promise<RegisteredAgent[]> {
    const ids = await this.pub.sMembers(this.keyAgents());
    if (ids.length === 0) return [];
    const raws = await this.pub.mGet(ids.map((id) => this.keyAgent(id)));
    const out: RegisteredAgent[] = [];
    const stale: string[] = [];
    raws.forEach((raw, i) => {
      if (!raw) {
        stale.push(ids[i]);
        return;
      }
      try {
        out.push(JSON.parse(raw) as RegisteredAgent);
      } catch { /* skip */ }
    });
    if (stale.length) await this.pub.sRem(this.keyAgents(), stale);
    return out;
  }

  // ---- 发布 ----

  async sendToAgent(instanceID: string, agentID: string, msg: proto.Message): Promise<void> {
    await this.pub.publish(this.chInstance(instanceID),
      JSON.stringify({ kind: "agent_msg", agent_id: agentID, msg } satisfies BusEnvelope));
  }

  async publishUserMessage(ownerID: string, msg: proto.Message): Promise<void> {
    await this.pub.publish(this.chUsers(),
      JSON.stringify({ kind: "user_msg", owner_id: ownerID, msg, src: this.instanceID } satisfies BusEnvelope));
  }

  async publishPending(reqID: string, msg: proto.Message): Promise<void> {
    await this.pub.publish(this.chUsers(),
      JSON.stringify({ kind: "pending", req_id: reqID, msg } satisfies BusEnvelope));
  }

  async publishAgentsChanged(): Promise<void> {
    await this.pub.publish(this.chUsers(),
      JSON.stringify({ kind: "agents_changed", src: this.instanceID } satisfies BusEnvelope));
  }
}
