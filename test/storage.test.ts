import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { LocalAttachmentStore, createLocalAttachmentStore, sanitizeFileName } from "../src/storage.ts";

test("local store put/get roundtrip", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "am-attach-"));
  try {
    const store = await createLocalAttachmentStore(dir);
    assert.ok(store);
    const url = await store.put("attachments/u1/abc/hello.png", Buffer.from("PNGDATA"), "image/png");
    assert.equal(url, "/files/attachments/u1/abc/hello.png");
    const obj = await store.get("attachments/u1/abc/hello.png");
    assert.equal(obj?.body.toString(), "PNGDATA");
    assert.equal(obj?.mime, "image/png");
    assert.equal(await store.get("attachments/u1/abc/missing.png"), undefined);
    assert.equal(await store.get("../secret"), undefined);

    assert.equal(await store.usage("attachments/u1"), 7);
    assert.equal(await store.usage("attachments/nobody"), 0);

    assert.equal(store.keyFromUrl(url), "attachments/u1/abc/hello.png");
    assert.equal(store.keyFromUrl("https://s3.example.com/bucket/x"), undefined);

    await store.delete("attachments/u1/abc/hello.png");
    assert.equal(await store.get("attachments/u1/abc/hello.png"), undefined);
    assert.equal(await store.usage("attachments/u1"), 0);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

test("unwritable dir disables store", async () => {
  const store = new LocalAttachmentStore("/proc/definitely-not-writable");
  await assert.rejects(store.init());
});

test("sanitizeFileName", () => {
  assert.equal(sanitizeFileName("hello world.png"), "hello_world.png");
  assert.equal(sanitizeFileName("截图 2026.png"), "截图_2026.png");
  assert.equal(sanitizeFileName("a/b/../c"), "a_b_.._c");
  assert.equal(sanitizeFileName(""), "file");
  assert.equal(sanitizeFileName("x".repeat(100)).length, 80);
});
