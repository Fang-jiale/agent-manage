#!/bin/bash
set -e

# End-to-end smoke test for agent-manage.
# Starts gateway (with MySQL), local-agent, and client, then sends a task
# through the admin WebSocket and verifies the messages were persisted.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GATEWAY_PORT=18080
LOCAL_AGENT_PORT=19001
JWT_SECRET=e2e-secret

PIDS=()
cleanup() {
    echo "cleaning up..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

echo "starting local-agent on $LOCAL_AGENT_PORT..."
node "$ROOT/src/local-agent.ts" -addr ":$LOCAL_AGENT_PORT" &
PIDS+=($!)
sleep 1

echo "starting gateway on $GATEWAY_PORT..."
AGENT_MANAGE_JWT_SECRET=$JWT_SECRET node "$ROOT/src/gateway.ts" -addr ":$GATEWAY_PORT" &
PIDS+=($!)
sleep 1.5

echo "logging in as admin..."
TOKEN=$(node "$ROOT/src/login.ts" -gateway "http://localhost:$GATEWAY_PORT" -name admin -password "${AGENT_MANAGE_ADMIN_PASSWORD:-admin123}")

echo "starting client..."
node "$ROOT/src/client.ts" -gateway "ws://localhost:$GATEWAY_PORT/ws/agent" -local-agent "http://localhost:$LOCAL_AGENT_PORT" -agent-id "e2e-agent" -token "$TOKEN" &
PIDS+=($!)
sleep 2

echo "sending task.create from admin WebSocket..."
TOKEN="$TOKEN" node - <<'NODE'
const WebSocket = require('ws');
const token = process.env.TOKEN;
const ws = new WebSocket('ws://localhost:18080/ws/admin?token=' + encodeURIComponent(token));
let accepted = false;
let done = false;
let rpcSeq = 0;
const pending = new Map();

function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = 'e2e-' + (++rpcSeq);
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
    setTimeout(() => reject(new Error('timeout ' + method)), 8000);
  });
}

ws.on('open', () => {
  ws.send(JSON.stringify({
    jsonrpc: '2.0',
    method: 'task.create',
    params: {
      agent_id: 'e2e-agent',
      task_id: 'task-e2e-1',
      type: 'chat',
      content: 'hello'
    },
    id: 'req-1'
  }));
});

ws.on('message', (data) => {
  const msg = JSON.parse(data);
  if (msg.id && pending.has(msg.id)) {
    const p = pending.get(msg.id);
    pending.delete(msg.id);
    msg.error ? p.reject(new Error(JSON.stringify(msg.error))) : p.resolve(msg.result);
    return;
  }
  if (msg.result && msg.result.status === 'accepted') {
    accepted = true;
  }
  if (msg.method === 'admin.task.progress' && msg.params.done) {
    done = true;
    verifyPersistence();
  }
});

async function verifyPersistence() {
  try {
    const list = await call('session.list', { agent_id: 'e2e-agent' });
    const session = (list.sessions || []).find(s => s.id === 'task-e2e-1-session');
    if (!session) throw new Error('session not persisted');
    if (session.message_count < 2) throw new Error('expected >=2 messages, got ' + session.message_count);
    const msgs = await call('message.list', { session_id: session.id });
    console.log('persisted messages:', msgs.messages.map(m => m.role).join(','));
    await call('session.delete', { id: session.id });
    ws.close();
    console.log('persistence verified');
  } catch (e) {
    console.error('persistence check failed:', e.message);
    process.exit(1);
  }
}

ws.on('error', (err) => {
  console.error('websocket error:', err);
  process.exit(1);
});

setTimeout(() => {
  if (!accepted || !done) {
    console.error('e2e timeout: accepted=' + accepted + ' done=' + done);
    process.exit(1);
  }
}, 15000);
NODE

echo "e2e test passed"
