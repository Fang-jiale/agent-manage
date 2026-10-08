import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { WebSocketServer } from 'ws';
import { normalizeGateway, gatewayBase } from '../src/client/gateway-url.ts';
import { ActivityStore } from '../src/client/activity.ts';
import { productCompatible, validateTargets } from '../src/product-platform.ts';
import { productDirPath, scanProductCatalog, validateProductManifest } from '../src/gateway/products.ts';
import bridge from '../desktop/bridge.cjs';

test('organization URLs preserve custom ports and proxy prefixes', () => {
  assert.equal(normalizeGateway('https://example.test:8443/team/'), 'wss://example.test:8443/team/ws/agent');
  assert.equal(normalizeGateway('wss://example.test/team/ws/agent'), 'wss://example.test/team/ws/agent');
  assert.equal(gatewayBase('wss://example.test:8443/team/ws/agent'), 'https://example.test:8443/team');
  assert.throws(() => normalizeGateway('https://name:password@example.test'));
  assert.throws(() => normalizeGateway('file:///tmp/config'));
});

test('Win7 x64 and modern packages are selected separately before install', () => {
  const legacy = { targets: [{ os: 'win32', arch: 'x64', min_os: '6.1', max_os: '6.1' }] };
  const modern = { targets: [{ os: 'win32', arch: 'x64', min_os: '10.0' }] };
  assert.equal(productCompatible(legacy, 'win32', 'x64', '6.1.7601'), true);
  assert.equal(productCompatible(legacy, 'win32', 'x64', '10.0.19045'), false);
  assert.equal(productCompatible(modern, 'win32', 'x64', '6.1.7601'), false);
  assert.equal(productCompatible(modern, 'linux', 'arm64', '5.4.0'), false);
  assert.throws(() => validateTargets([{ os: 'linux', arch: 'arm64', min_os: 5 }]));
});

test('activities survive process restart and interrupted operations are not reported complete', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'ywm-activity-test-'));
  try {
    const file = path.join(dir, 'activity.json');
    const store = new ActivityStore(file);
    const first = store.start('安装智能体');
    store.update(first, '下载中');
    const second = store.start('连接组织');
    store.update(second, '已连接', 'done');
    const records = new ActivityStore(file).list();
    assert.equal(records.find(item => item.id === first).status, 'failed');
    assert.equal(records.find(item => item.id === second).status, 'done');
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('desktop bridge restricts paths, methods and external schemes', () => {
  assert.equal(bridge.validateRequest('/api/state').method, 'GET');
  assert.equal(bridge.validateRequest('/api/pair', { method: 'POST', body: '{}' }).contentType, 'application/json');
  assert.throws(() => bridge.validateRequest('https://other.test/api/state'));
  assert.throws(() => bridge.validateRequest('/api/products/../../state'));
  assert.throws(() => bridge.validateRequest('/api/state', { method: 'CONNECT' }));
  assert.throws(() => bridge.externalUrl('file:///etc/passwd'));
  assert.throws(() => bridge.externalUrl('javascript:alert(1)'));
});

test('same product version retains independent architecture artifacts', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'ywm-artifacts-test-'));
  try {
    for (const artifact of ['win7-x64', 'linux-arm64']) {
      const dir = productDirPath(root, 'demo', '1.0.0', artifact);
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(path.join(dir, 'manifest.json'), JSON.stringify({ format: 1, brand: 'demo', version: '1.0.0', kind: 'stdio', artifact_id: artifact }));
      await fs.writeFile(path.join(dir, 'meta.json'), JSON.stringify({ sha256: 'a'.repeat(64), size: 1, uploaded_at: Date.now() }));
    }
    assert.deepEqual(scanProductCatalog(root).map(item => item.artifact_id).sort(), ['linux-arm64', 'win7-x64']);
    assert.equal(productDirPath(root, 'demo', '1.0.0', '../escape'), null);
    assert.throws(() => validateProductManifest({ format: 1, brand: 'demo', version: '1.0.0', kind: 'stdio', artifact_id: '../escape' }));
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

test('real client API: protected desktop port, async pairing, approval and persisted credential', { timeout: 20000 }, async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'ywm-desktop-api-test-'));
  const gateway = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await once(gateway, 'listening');
  let pairingSocket;
  gateway.on('connection', ws => ws.on('message', raw => {
    const msg = JSON.parse(raw.toString());
    if (msg.method === 'connector.pair') {
      pairingSocket = ws;
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { status: 'pending' } }));
    } else if (msg.id) ws.send(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
  }));
  const child = spawn(process.execPath, ['src/client.ts', '-ui-addr', '127.0.0.1:0', '-config', path.join(dir, 'connector.json'), '-products-dir', path.join(dir, 'products')], {
    cwd: path.resolve(import.meta.dirname, '..'), stdio: ['ignore', 'pipe', 'pipe', 'ipc'], env: { ...process.env, AGENT_MANAGE_UI_TOKEN: 'test-token' }
  });
  let errors = ''; child.stderr.on('data', part => { errors += part; });
  child.stdout.resume();
  try {
    const ready = await Promise.race([once(child, 'message').then(([message]) => message), once(child, 'exit').then(() => { throw Error(errors); })]);
    const base = 'http://127.0.0.1:' + ready.port;
    assert.equal((await fetch(base + '/api/state')).status, 403);
    const headers = { 'x-ywm-client-token': 'test-token', 'Content-Type': 'application/json' };
    assert.equal((await fetch(base + '/api/state', { headers })).status, 200);
    const paired = await fetch(base + '/api/pair', { method: 'POST', headers, body: JSON.stringify({ gateway: 'http://127.0.0.1:' + gateway.address().port, code: 'fixture-code' }) });
    assert.equal(paired.status, 202);
    for (let i = 0; i < 30 && !pairingSocket; i++) await new Promise(resolve => setTimeout(resolve, 50));
    const pending = await (await fetch(base + '/api/state', { headers })).json();
    assert.equal(pending.pairing.status, 'pending');
    pairingSocket.send(JSON.stringify({ jsonrpc: '2.0', method: 'connector.credential', params: { key: 'amk_fixture' } }));
    let connected;
    for (let i = 0; i < 50; i++) {
      connected = await (await fetch(base + '/api/state', { headers })).json();
      if (connected.connected) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(connected.connected, true);
    assert.equal(connected.configured, true);
    const activity = await (await fetch(base + '/api/activities', { headers })).json();
    assert.equal(activity.activities[0].status, 'done');
    assert.equal(JSON.parse(await fs.readFile(path.join(dir, 'connector.json'), 'utf8')).key, 'amk_fixture');
    const nextReady = once(child, 'message');
    const reset = await fetch(base + '/api/connection/reset', { method: 'POST', headers });
    assert.equal(reset.status, 200);
    const [newAddress] = await nextReady;
    const fresh = await (await fetch('http://127.0.0.1:' + newAddress.port + '/api/state', { headers })).json();
    assert.equal(fresh.configured, false);
    assert.equal(JSON.parse(await fs.readFile(path.join(dir, 'connector.json.previous'), 'utf8')).key, 'amk_fixture');
  } finally {
    child.kill('SIGTERM');
    for (const socket of gateway.clients) socket.terminate();
    await new Promise(resolve => gateway.close(resolve));
    if (child.exitCode === null) await Promise.race([once(child, 'exit'), new Promise(resolve => setTimeout(() => { child.kill('SIGKILL'); resolve(); }, 2000))]);
    await fs.rm(dir, { recursive: true, force: true });
  }
});
