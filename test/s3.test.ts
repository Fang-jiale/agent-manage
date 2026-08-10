// S3 存储路径集成测试。需要 MinIO（或任意 S3 兼容服务），通过
// AGENT_MANAGE_TEST_S3_ENDPOINT 指定，例如本地：
//   docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
//   AGENT_MANAGE_TEST_S3_ENDPOINT=http://localhost:9000 node --test test/s3.test.ts
// 未设置时整组跳过。
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { createS3AttachmentStore } from "../src/storage.ts";

const ENDPOINT = process.env.AGENT_MANAGE_TEST_S3_ENDPOINT ?? "";

async function newStore(t: import("node:test").TestContext) {
  const store = await createS3AttachmentStore({
    endpoint: ENDPOINT,
    region: process.env.AGENT_MANAGE_TEST_S3_REGION ?? "us-east-1",
    bucket: `ywm-test-${crypto.randomUUID().slice(0, 8)}`,
    accessKey: process.env.AGENT_MANAGE_TEST_S3_ACCESS_KEY ?? "minioadmin",
    secretKey: process.env.AGENT_MANAGE_TEST_S3_SECRET_KEY ?? "minioadmin",
  });
  if (!store) {
    t.skip("S3 不可用（检查 AGENT_MANAGE_TEST_S3_ENDPOINT 与凭据）");
    return undefined;
  }
  return store;
}

test("s3 put/keyFromUrl/delete roundtrip", async (t) => {
  if (ENDPOINT === "") {
    t.skip("未设置 AGENT_MANAGE_TEST_S3_ENDPOINT，跳过 S3 测试");
    return;
  }
  const store = await newStore(t);
  if (!store) return;

  const key = `attachments/u-test/${crypto.randomUUID()}/hello.txt`;
  const url = await store.put(key, Buffer.from("hello s3"), "text/plain");
  assert.ok(url.endsWith(`/${key}`), `url: ${url}`);

  // keyFromUrl 应能从完整 URL 还原 key（级联删除依赖）
  assert.equal(store.keyFromUrl(url), key);
  assert.equal(store.keyFromUrl("https://other-host.com/x/y"), undefined);

  // 匿名读策略生效：上传后可直接 GET
  const got = await fetch(url);
  assert.equal(got.status, 200);
  assert.equal(await got.text(), "hello s3");

  await store.delete(key);
  const after = await fetch(url);
  assert.equal(after.status, 404);
});
