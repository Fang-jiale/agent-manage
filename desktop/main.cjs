const { app, BrowserWindow, Tray, Menu, nativeImage, ipcMain, dialog, shell } = require('electron');
const { spawn, execFile } = require('node:child_process');
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs');
const crypto = require('node:crypto');
const http = require('node:http');
const { fileURLToPath } = require('node:url');
const { validateRequest, externalUrl } = require('./bridge.cjs');

const hasLock = app.requestSingleInstanceLock();
let win, tray, child, port, quitting = false, restarting = false, startupTimer;
const sessionToken = crypto.randomBytes(32).toString('hex');
const developmentTarget = process.platform === 'darwin' ? 'mac-' + process.arch : process.platform === 'win32' ? 'win-x64' : process.arch === 'arm64' ? 'linux-arm64' : 'linux-x64-web';
const coreRoot = app.isPackaged ? path.join(process.resourcesPath, 'core') : path.resolve(process.env.YWM_DESKTOP_CORE || path.join(__dirname, '../dist/desktop-resources', developmentTarget, 'core'));
const pagePath = path.join(coreRoot, 'static', 'client.html');
const configPath = process.env.AGENT_MANAGE_CONFIG || path.join(os.homedir(), '.agent-manage', 'connector.json');
const logDir = path.join(path.dirname(configPath), 'logs');
let preferences = { startAtLogin: false, closeToTray: true };
function preferenceFile() { return path.join(app.getPath('userData'), 'desktop-settings.json'); }
function showWindow() { if (win) { win.show(); if (win.isMinimized()) win.restore(); win.focus(); } }
function trusted(event) {
  if (!win || event.sender !== win.webContents || event.senderFrame !== event.sender.mainFrame) throw Error('Untrusted sender');
  if (fileURLToPath(event.senderFrame.url.split('?')[0]) !== pagePath) throw Error('Untrusted page');
}
function request(endpoint, options = {}) {
  const safe = validateRequest(endpoint, options);
  if (!port) return Promise.reject(Error('后台服务正在启动，请稍候；若持续失败可在设置中重启服务。'));
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, path: endpoint, method: safe.method,
      headers: { 'x-ywm-client-token': sessionToken, 'Content-Type': safe.contentType, ...(safe.body ? { 'Content-Length': Buffer.byteLength(safe.body) } : {}) } }, res => {
      const chunks = []; let size = 0;
      res.on('data', chunk => { size += chunk.length; if (size > 8 * 1024 * 1024) { res.destroy(); reject(Error('本地响应过大')); } else chunks.push(chunk); });
      res.on('error', reject);
      res.on('end', () => {
        try { const value = JSON.parse(Buffer.concat(chunks).toString()); if (res.statusCode >= 400) reject(Error(value.error || '操作失败')); else resolve(value); }
        catch (error) { reject(error); }
      });
    });
    req.setTimeout(360_000, () => req.destroy(Error('本地操作超时，请在活动记录中检查结果')));
    req.on('error', reject);
    req.end(safe.body);
  });
}
function startCore() {
  const executable = process.env.YWM_DESKTOP_NODE || path.join(coreRoot, 'runtime', process.platform === 'win32' ? 'node.exe' : 'node');
  if (!fs.existsSync(executable)) { dialog.showErrorBox('缺少客户端运行时', '请重新安装完整客户端，或运行桌面资源准备命令。'); return; }
  fs.mkdirSync(logDir, { recursive: true });
  const logPath = path.join(logDir, 'desktop-core.log');
  if (fs.existsSync(logPath) && fs.statSync(logPath).size > 5 * 1024 * 1024) fs.copyFileSync(logPath, logPath + '.previous');
  const log = fs.createWriteStream(logPath, { flags: 'w', mode: 0o600 });
  child = spawn(executable, [path.join(coreRoot, 'worker.mjs'), '-config', configPath, '-ui-addr', '127.0.0.1:0'], {
    cwd: coreRoot, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { ...process.env, AGENT_MANAGE_UI_TOKEN: sessionToken },
  });
  const running = child;
  child.stdout.pipe(log, { end: false }); child.stderr.pipe(log, { end: false });
  child.on('message', message => {
    if (message?.type === 'client.ready' && Number.isInteger(message.port)) { port = message.port; clearTimeout(startupTimer); }
  });
  child.on('error', error => dialog.showErrorBox('后台服务启动失败', error.message));
  child.on('exit', () => {
    log.end(); clearTimeout(startupTimer);
    if (child === running) { child = null; port = null; }
    if (!quitting && !restarting) {
      dialog.showMessageBox(win, { type: 'warning', message: '后台服务已停止', detail: '可在设置中重启后台服务，已保存的接入信息和智能体安装仍会保留。', buttons: ['知道了'] });
      showWindow();
    }
  });
  startupTimer = setTimeout(() => { if (!port && !quitting) { showWindow(); dialog.showErrorBox('后台服务启动超时', '请在设置中导出诊断信息，或重启后台服务。'); } }, 25_000);
}
async function stopCore() {
  const running = child;
  if (!running || running.exitCode !== null) return;
  await new Promise(resolve => {
    const timer = setTimeout(() => {
      if (process.platform === 'win32') execFile(path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'taskkill.exe'), ['/PID', String(running.pid), '/T', '/F'], { windowsHide: true }, () => resolve());
      else { running.kill('SIGKILL'); resolve(); }
    }, 10_000);
    running.once('exit', () => { clearTimeout(timer); resolve(); });
    if (running.connected) running.send({ type: 'client.stop' }); else running.kill('SIGTERM');
  });
}
if (!hasLock) app.quit();
else {
  app.on('second-instance', showWindow);
  app.on('activate', showWindow);
  app.on('window-all-closed', () => { if (!tray) app.quit(); });
  app.on('before-quit', event => {
    if (quitting) return;
    event.preventDefault(); quitting = true;
    void stopCore().finally(() => app.quit());
  });
  app.whenReady().then(() => {
    try { preferences = { ...preferences, ...JSON.parse(fs.readFileSync(preferenceFile(), 'utf8')) }; } catch {}
    win = new BrowserWindow({ width: 1140, height: 820, minWidth: 700, minHeight: 540, title: 'YwMatrix 终端',
      webPreferences: { preload: path.join(__dirname, 'preload.cjs'), contextIsolation: true, nodeIntegration: false, sandbox: true } });
    win.setMenuBarVisibility(false);
    win.webContents.on('will-navigate', event => event.preventDefault());
    win.webContents.setWindowOpenHandler(({ url }) => { try { void shell.openExternal(externalUrl(url)); } catch {} return { action: 'deny' }; });
    win.webContents.session.setPermissionRequestHandler((_contents, _permission, callback) => callback(false));
    win.on('close', event => { if (!quitting && preferences.closeToTray && tray) { event.preventDefault(); win.hide(); } });
    try {
      const image = nativeImage.createFromPath(path.join(coreRoot, 'static', 'icon-192.png')).resize({ width: 20, height: 20 });
      tray = new Tray(image); tray.setToolTip('YwMatrix · 智能体在后台运行'); tray.on('click', showWindow);
      tray.setContextMenu(Menu.buildFromTemplate([{ label: '打开 YwMatrix', click: showWindow }, { type: 'separator' }, { label: '退出并停止智能体', click: () => app.quit() }]));
    } catch { tray = null; }
    ipcMain.handle('client:request', (event, endpoint, options) => { trusted(event); return request(endpoint, options); });
    ipcMain.handle('desktop:directory', async event => { trusted(event); const result = await dialog.showOpenDialog(win, { properties: ['openDirectory', 'createDirectory'] }); return result.canceled ? null : result.filePaths[0]; });
    ipcMain.handle('desktop:external', (event, url) => { trusted(event); return shell.openExternal(externalUrl(url)); });
    ipcMain.handle('desktop:settings', (event, options) => {
      trusted(event);
      if (options) {
        if (typeof options.startAtLogin === 'boolean' && ['win32', 'darwin'].includes(process.platform)) {
          app.setLoginItemSettings({ openAtLogin: options.startAtLogin }); preferences.startAtLogin = options.startAtLogin;
        }
        if (typeof options.closeToTray === 'boolean') preferences.closeToTray = options.closeToTray;
        fs.mkdirSync(app.getPath('userData'), { recursive: true }); fs.writeFileSync(preferenceFile(), JSON.stringify(preferences));
      }
      return { ...preferences, trayAvailable: !!tray, supportsAutoStart: ['win32', 'darwin'].includes(process.platform), version: app.getVersion() };
    });
    ipcMain.handle('desktop:restart', async event => { trusted(event); if (restarting) return; restarting = true; await stopCore(); startCore(); restarting = false; });
    ipcMain.handle('desktop:diagnostics', async event => {
      trusted(event);
      const status = await request('/api/state').catch(() => null);
      const payload = { version: app.getVersion(), platform: process.platform, arch: process.arch, connected: status?.connected ?? false,
        pairing: status?.pairing?.status, agents: (status?.agents || []).map(a => ({ state: a.state, conn_type: a.conn_type })),
        coreRunning: !!child, timestamp: new Date().toISOString() };
      const result = await dialog.showSaveDialog(win, { defaultPath: 'ywmatrix-diagnostics.json' });
      if (!result.canceled && result.filePath) fs.writeFileSync(result.filePath, JSON.stringify(payload, null, 2), { mode: 0o600 });
      return !result.canceled;
    });
    startCore();
    void win.loadFile(pagePath);
  });
}
