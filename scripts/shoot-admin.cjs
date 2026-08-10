// Headless-Chrome CDP screenshot helper for the admin console.
// Usage: BASE=http://localhost:18080 LOGIN=$(cat /tmp/admin-login.json) node scripts/shoot-admin.cjs <light|dark> <outdir>
const WebSocket = require('ws');
const fs = require('fs');
const http = require('http');

const BASE = process.env.BASE || 'http://localhost:18080';
const login = JSON.parse(process.env.LOGIN);
const theme = process.argv[2] || 'light';
const outdir = process.argv[3] || '/tmp/admin-shots';

function getJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let buf = '';
      res.on('data', (c) => buf += c);
      res.on('end', () => { try { resolve(JSON.parse(buf)); } catch (e) { reject(e); } });
    }).on('error', reject);
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  fs.mkdirSync(outdir, { recursive: true });
  const targets = await getJson('http://127.0.0.1:9222/json/list');
  const page = targets.find((t) => t.type === 'page');
  if (!page) throw new Error('no page target');
  const ws = new WebSocket(page.webSocketDebuggerUrl, { perMessageDeflate: false, maxPayload: 64 * 1024 * 1024 });
  let seq = 0;
  const pending = new Map();
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++seq;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
    setTimeout(() => { if (pending.has(id)) { pending.delete(id); reject(new Error('cdp timeout ' + method)); } }, 15000);
  });
  ws.on('message', (d) => {
    const m = JSON.parse(d);
    if (m.id && pending.has(m.id)) {
      const p = pending.get(m.id);
      pending.delete(m.id);
      m.error ? p.reject(new Error(JSON.stringify(m.error))) : p.resolve(m.result);
    }
  });
  await new Promise((r, j) => { ws.on('open', r); ws.on('error', j); });

  await send('Page.enable');
  await send('Emulation.setDeviceMetricsOverride', { width: 1440, height: 900, deviceScaleFactor: 1, mobile: false });

  // index.html 在 pagehide 时会把内存 state 回写 localStorage，会覆盖我们的写入；
  // 改为在同源无 JS 的 /healthz 上播种，再进 /admin
  await send('Page.navigate', { url: BASE + '/healthz' });
  await sleep(1200);
  const stored = JSON.stringify({ token: login.token, user: login.user, settings: { themeMode: theme } });
  await send('Runtime.evaluate', { expression: `localStorage.setItem('agent_manage_v1', ${JSON.stringify(stored)})` });

  await send('Page.navigate', { url: BASE + '/admin' });
  await sleep(3000);

  const url = await send('Runtime.evaluate', { expression: 'location.pathname', returnByValue: true });
  console.log('landed on:', url.result.value);
  const status = await send('Runtime.evaluate', { expression: `document.getElementById('connStatus')?.textContent`, returnByValue: true });
  console.log('conn status:', status.result.value);

  const shots = [['overview', null], ['users', 'users'], ['agents', 'agents'], ['keys', 'keys']];
  for (const [name, tab] of shots) {
    if (tab) {
      await send('Runtime.evaluate', { expression: `document.querySelector('.admin-nav [data-tab="${tab}"]')?.click()` });
      await sleep(1800);
    }
    const shot = await send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync(`${outdir}/${theme}-${name}.png`, Buffer.from(shot.data, 'base64'));
    console.log('saved', `${outdir}/${theme}-${name}.png`);
  }
  ws.close();
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
