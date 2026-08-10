import { test } from "node:test";
import assert from "node:assert/strict";
import * as proto from "../src/protocol.ts";

test("newRequest", () => {
  const msg = proto.newRequest("req-1", proto.METHOD_TASK_CREATE, {
    agent_id: "agent-1",
    task_id: "task-1",
    type: "chat",
    content: "hello",
  } satisfies proto.TaskCreateParams);

  assert.equal(msg.jsonrpc, proto.VERSION);
  assert.equal(msg.id, "req-1");
  assert.equal(msg.method, proto.METHOD_TASK_CREATE);

  const params = proto.decodeParams<proto.TaskCreateParams>(msg);
  assert.equal(params.agent_id, "agent-1");
  assert.equal(params.content, "hello");
});

test("newNotification", () => {
  const msg = proto.newNotification(proto.METHOD_ADMIN_PROGRESS, {
    task_id: "task-1",
    agent_id: "agent-1",
    done: true,
  } satisfies proto.AdminProgressParams);

  assert.equal(msg.id, undefined);
  assert.equal(msg.method, proto.METHOD_ADMIN_PROGRESS);
  assert.ok(proto.isNotification(msg));
});

test("newResponse", () => {
  const msg = proto.newResponse("req-1", {
    status: "accepted",
    task_id: "task-1",
  } satisfies proto.TaskAcceptResult);

  assert.equal(msg.id, "req-1");
  const result = msg.result as proto.TaskAcceptResult;
  assert.equal(result.status, "accepted");
  assert.equal(result.task_id, "task-1");
});

test("newErrorResponse", () => {
  const msg = proto.newErrorResponse("req-1", proto.ERR_AGENT_NOT_FOUND, "agent not found");

  assert.equal(msg.id, "req-1");
  assert.equal(msg.error?.code, proto.ERR_AGENT_NOT_FOUND);
  assert.equal(msg.error?.message, "agent not found");
  assert.equal(msg.result, undefined);
});

test("message round-trips through JSON", () => {
  const original = proto.newNotification(proto.METHOD_PROGRESS, {
    token: "task-1",
    value: {
      kind: proto.PROGRESS_KIND_REPORT,
      agent_id: "agent-1",
      task_id: "task-1",
      content: proto.textContent("hi"),
    },
  } satisfies proto.ProgressParams);

  const parsed = JSON.parse(JSON.stringify(original)) as proto.Message;
  assert.equal(parsed.jsonrpc, proto.VERSION);
  assert.equal(parsed.method, proto.METHOD_PROGRESS);
  const params = proto.decodeParams<proto.ProgressParams>(parsed);
  assert.equal(params.value?.content?.[0].text, "hi");
});
