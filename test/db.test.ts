import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { Db } from "../src/db.ts";
import { hashPassword } from "../src/auth.ts";

const URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";

async function freshDb(t: import("node:test").TestContext): Promise<Db | undefined> {
  const db = new Db(URL);
  try {
    await db.init();
    return db;
  } catch {
    t.skip("MySQL 不可用，跳过存储层测试");
    await db.close().catch(() => {});
    return undefined;
  }
}

test("user/session/message roundtrip", async (t) => {
  const db = await freshDb(t);
  if (!db) return;
  const uid = "test-" + crypto.randomUUID();
  const sid = crypto.randomUUID();
  try {
    await db.createUser({ id: uid, name: uid, password_hash: hashPassword("pw") });
    const u = await db.getUserByName(uid);
    assert.equal(u?.id, uid);
    assert.equal((await db.getUserById(uid))?.name, uid);

    await db.createSession({ id: sid, owner_id: uid, agent_id: "agent-x", title: "t" });
    assert.equal((await db.getSession(uid, sid))?.title, "t");
    assert.equal(await db.getSession("other", sid), undefined);

    await db.renameSession(uid, sid, "t2");
    assert.equal((await db.getSession(uid, sid))?.title, "t2");
    assert.equal(await db.renameSession("other", sid, "x"), false);

    for (let i = 0; i < 5; i++) {
      await db.appendMessage({
        id: crypto.randomUUID(), session_id: sid, owner_id: uid, agent_id: "agent-x",
        role: i % 2 ? "assistant" : "user", content: JSON.stringify({ text: `m${i}` }),
        task_id: "task", created_at: 1000 + i,
      });
    }
    assert.equal(await db.countMessages(uid, sid), 5);

    const page1 = await db.listMessages(uid, sid, 2);
    assert.deepEqual(page1.map((m) => JSON.parse(m.content).text), ["m3", "m4"]);
    const page2 = await db.listMessages(uid, sid, 2, page1[0].created_at);
    assert.deepEqual(page2.map((m) => JSON.parse(m.content).text), ["m1", "m2"]);

    const sessions = await db.listSessions(uid, "agent-x");
    assert.equal(sessions.length, 1);
    assert.equal(Number(sessions[0].message_count), 5);

    const lastMsgs = await db.listLastMessages(uid);
    const last = lastMsgs.find((m) => m.session_id === sid);
    assert.equal(last?.role, "user");
    assert.ok(JSON.parse(last!.content).text.includes("m4"));

    assert.equal(await db.deleteSession(uid, sid), true);
    assert.equal((await db.listMessages(uid, sid, 10)).length, 0);
    assert.equal(await db.deleteSession(uid, sid), false);

    // 保留策略：updated_at 早于 cutoff 的会话可被捞出
    await db.createSession({ id: sid, owner_id: uid, agent_id: "agent-x", title: "old" });
    assert.ok((await db.listOldSessions(Date.now() + 1000)).some((s) => s.id === sid));
    assert.ok(!(await db.listOldSessions(0)).some((s) => s.id === sid));
  } finally {
    await db.deleteSession(uid, sid).catch(() => {});
    await db.deleteUser(uid).catch(() => {});
    await db.close();
  }
});

test("user management", async (t) => {
  const db = await freshDb(t);
  if (!db) return;
  const uid = "test-" + crypto.randomUUID();
  try {
    await db.createUser({ id: uid, name: uid, password_hash: hashPassword("pw1"), role: "admin" });
    let u = await db.getUserById(uid);
    assert.equal(u?.role, "admin");
    assert.equal(u?.disabled, 0);

    assert.equal(await db.setUserDisabled(uid, true), true);
    u = await db.getUserById(uid);
    assert.equal(u?.disabled, 1);
    assert.equal(await db.setUserDisabled("nonexistent", true), false);

    assert.equal(await db.setUserPassword(uid, hashPassword("pw2")), true);
    assert.notEqual((await db.getUserById(uid))?.password_hash, u?.password_hash);

    const users = await db.listUsers();
    assert.ok(users.some((x) => x.id === uid));

    await db.touchLastLogin(uid);
    u = await db.getUserById(uid);
    assert.ok(u?.last_login_at !== null && Number(u?.last_login_at) > 0);

    assert.equal(await db.setUserRole(uid, "user"), true);
    assert.equal((await db.getUserById(uid))?.role, "user");
    assert.equal(await db.setUserRole("nonexistent", "admin"), false);

    // 分页 + 搜索
    const uid2 = "test-" + crypto.randomUUID();
    await db.createUser({ id: uid2, name: uid2, password_hash: hashPassword("pw") });
    const all = await db.listUsersPaged({ limit: 100, offset: 0 });
    assert.ok(all.users.some((x) => x.id === uid));
    assert.ok(all.total >= 2);
    const filtered = await db.listUsersPaged({ query: uid2, limit: 10, offset: 0 });
    assert.equal(filtered.total, 1);
    assert.equal(filtered.users[0]?.id, uid2);
    await db.deleteUser(uid2).catch(() => {});
  } finally {
    await db.deleteUser(uid).catch(() => {});
    await db.close();
  }
});

test("agents registry roundtrip", async (t) => {
  const db = await freshDb(t);
  if (!db) return;
  const owner = "test-" + crypto.randomUUID();
  const agentID = "test-agent-" + crypto.randomUUID();
  try {
    await db.upsertAgent({
      id: agentID, owner_id: owner, name: "A1",
      platform: JSON.stringify({ os: "darwin" }),
      capabilities: JSON.stringify([{ type: "chat", name: "general" }]),
      status: "online",
    });
    let page = await db.listAgentsPaged({ ownerID: owner, limit: 10, offset: 0 });
    assert.equal(page.total, 1);
    assert.equal(page.agents[0].id, agentID);
    assert.equal(page.agents[0].status, "online");

    // upsert 更新归属与状态（凭证即权威）
    await db.upsertAgent({ id: agentID, owner_id: owner, name: "A1b", platform: null, capabilities: null, status: "busy" });
    page = await db.listAgentsPaged({ query: agentID, limit: 10, offset: 0 });
    assert.equal(page.agents[0].name, "A1b");
    assert.equal(page.agents[0].status, "busy");

    await db.touchAgent(agentID, "online");
    await db.markAgentOffline(agentID);
    page = await db.listAgentsPaged({ status: "offline", query: agentID, limit: 10, offset: 0 });
    assert.equal(page.total, 1);
    page = await db.listAgentsPaged({ status: "online", query: agentID, limit: 10, offset: 0 });
    assert.equal(page.total, 0);

    assert.ok((await db.countAgents()) >= 1);

    const owner2 = "test-" + crypto.randomUUID();
    assert.equal(await db.reassignAgent(agentID, owner2), true);
    page = await db.listAgentsPaged({ ownerID: owner2, limit: 10, offset: 0 });
    assert.equal(page.total, 1);
    assert.equal(await db.reassignAgent("nonexistent", owner2), false);
  } finally {
    await db.close();
  }
});

test("device keys roundtrip", async (t) => {
  const db = await freshDb(t);
  if (!db) return;
  const owner = "test-" + crypto.randomUUID();
  const keyID = crypto.randomUUID();
  const hash = crypto.randomBytes(32).toString("hex");
  try {
    await db.createDeviceKey({ id: keyID, owner_id: owner, name: "laptop", key_hash: hash });

    const byHash = await db.getDeviceKeyByHash(hash);
    assert.equal(byHash?.id, keyID);
    assert.equal(byHash?.owner_id, owner);
    assert.equal(byHash?.disabled, 0);
    assert.equal(await db.getDeviceKeyByHash("0".repeat(64)), undefined);

    await db.touchDeviceKeyUsed(keyID);
    let keys = await db.listDeviceKeys(owner);
    assert.equal(keys.length, 1);
    assert.ok(keys[0].last_used_at !== null && Number(keys[0].last_used_at) > 0);

    assert.equal(await db.setDeviceKeyDisabled(keyID, true), true);
    keys = await db.listDeviceKeys(owner);
    assert.equal(keys[0].disabled, 1);
    assert.equal(await db.setDeviceKeyDisabled("nonexistent", true), false);

    assert.equal((await db.listDeviceKeys("other")).length, 0);
  } finally {
    await db.close();
  }
});
