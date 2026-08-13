import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import * as proto from "../src/protocol.ts";
import { createGatewayServer, type GatewayConfig } from "../src/gateway.ts";
import { createLocalAttachmentStore } from "../src/storage.ts";
import { Db } from "../src/db.ts";
import { signJwt } from "../src/auth.ts";
import { setLogLevel } from "../src/util.ts";

setLogLevel("error");

const STATIC_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
const JWT_SECRET = "test-secret";
const DB_URL = process.env.AGENT_MANAGE_TEST_DATABASE_URL
  ?? "mysql://ywmatrix:ywmatrix_dev@localhost:3306/ywmatrix";

// 删除会话应级联删除消息引用的附件文件（需要真实 MySQL，无则跳过）
test("session.delete cascades attachment files", async (t) => {
  const db = new Db(DB_URL);
  try {
    await db.init();
  } catch {
    t.skip("MySQL 不可用，跳过");
    await db.close().catch(() => {});
    return;
  }
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "am-cascade-"));
  const store = await createLocalAttachmentStore(dir);
  assert.ok(store);
  const cfg: GatewayConfig = {
    addr: ":0", logLevel: "error", agentTimeoutMs: 90_000, userTimeoutMs: 120_000,
    taskTimeoutMs: 300_000, databaseURL: DB_URL, jwtSecret: JWT_SECRET, jwtTtlMs: 60_000,
    adminPassword: "x", redisURL: "", redisPrefix: "ywm", instanceID: "test-cascade",
    attachDir: dir, attachQuotaMb: 0, retentionDays: 0,
    s3Endpoint: "", s3Region: "us-east-1", s3Bucket: "ywmatrix",
    s3AccessKey: "", s3SecretKey: "", s3PublicURL: "",
    oidcIssuer: "", oidcClientID: "", oidcClientSecret: "", oidcRedirectURL: "",
    oidcEmployeeClaim: "employee_id",
    trustProxy: false,
  };
  const { server } = await createGatewayServer(cfg, STATIC_FILE, db, store);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const port = (server.address() as AddressInfo).port;
  const base = `http://localhost:${port}`;
  const token = signJwt({ sub: "u-cascade-test", name: "cascade" }, JWT_SECRET, 60_000);
  const sessionID = crypto.randomUUID();

  try {
    await db.createUser({ id: "u-cascade-test", name: "u-cascade-test", password_hash: "x" });
    // 上传附件
    const up = await fetch(`${base}/attachments`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ name: "c.png", mime: "image/png", data: "aGVsbG8=" }),
    });
    const { url } = (await up.json()) as { url: string };
    assert.ok(url.startsWith("/files/"));
    const key = store.keyFromUrl(url);
    assert.ok(key);
    assert.ok(await store.get(key), "file should exist before delete");

    // 写入一条带附件引用的用户消息 + 会话
    await db.createSession({ id: sessionID, owner_id: "u-cascade-test", agent_id: "agent-x", title: "cascade" });
    await db.appendMessage({
      id: crypto.randomUUID(), session_id: sessionID, owner_id: "u-cascade-test",
      agent_id: "agent-x", role: "user",
      content: JSON.stringify({ text: "hi", attachments: [{ name: "c.png", mime: "image/png", size: 5, url }] }),
      task_id: "t-cascade",
    });

    // WS 调 session.delete
    const ws = new WebSocket(`ws://localhost:${port}/ws/admin?token=${token}`);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", reject);
    });
    ws.send(JSON.stringify(proto.newRequest("d1", proto.METHOD_SESSION_DELETE, { id: sessionID })));
    const resp = await new Promise<proto.Message>((resolve) => {
      ws.on("message", (d) => {
        const m = JSON.parse(d.toString()) as proto.Message;
        if (m.id === "d1") resolve(m);
      });
    });
    assert.equal(resp.error, undefined);
    ws.close();

    // 会话、消息、附件文件都应消失
    assert.equal(await db.getSession("u-cascade-test", sessionID), undefined);
    assert.equal(await store.get(key), undefined, "attachment file should be deleted");
  } finally {
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
    await db.deleteSession("u-cascade-test", sessionID).catch(() => {});
    await db.deleteUser("u-cascade-test").catch(() => {});
    await db.close();
    await fs.rm(dir, { recursive: true, force: true });
  }
});
