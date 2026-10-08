import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { build } from 'esbuild';
import { createRequire } from 'node:module';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const appVersion = JSON.parse(fs.readFileSync(path.join(root, 'desktop/package.json'), 'utf8')).version;
const target = process.argv[2];
const prepareOnly = process.argv.includes('--prepare-only');
const archiveFlags = process.platform === 'darwin' ? ['--no-xattrs'] : [];
const runtimes = JSON.parse(fs.readFileSync(path.join(root, 'desktop/runtime-lock.json'), 'utf8'));
if (!runtimes[target]) throw Error('Target required: ' + Object.keys(runtimes).join(', '));
const runtime = runtimes[target];
const cache = path.join(root, 'dist/desktop-runtime');
const core = path.join(root, 'dist/desktop-resources', target, 'core');
fs.mkdirSync(cache, { recursive: true });
fs.mkdirSync(path.join(core, 'runtime'), { recursive: true });
fs.mkdirSync(path.join(core, 'static'), { recursive: true });
const archive = path.join(cache, runtime.archive);
if (!fs.existsSync(archive)) {
  const sources = [runtime.url];
  if (runtime.url.startsWith('https://nodejs.org/dist/')) sources.push(runtime.url.replace('https://nodejs.org/dist/', 'https://registry.npmmirror.com/-/binary/node/'));
  let error;
  for (const source of sources) {
    try {
      execFileSync('curl', ['-fsSL', '--connect-timeout', '15', '--max-time', '180', '--speed-limit', '65536', '--speed-time', '20', source, '-o', archive + '.download'], { stdio: 'inherit' });
      const digest = crypto.createHash('sha256').update(fs.readFileSync(archive + '.download')).digest('hex');
      if (digest !== runtime.sha256) throw Error('Downloaded runtime checksum mismatch');
      fs.renameSync(archive + '.download', archive); error = null; break;
    } catch (failure) { error = failure; console.warn('Runtime source unavailable:', new URL(source).hostname); }
  }
  if (error) throw error;
}
const actual = crypto.createHash('sha256').update(fs.readFileSync(archive)).digest('hex');
if (actual !== runtime.sha256) throw Error('Runtime checksum mismatch: ' + runtime.archive);
const extraction = fs.mkdtempSync(path.join(cache, 'extract-'));
if (archive.endsWith('.zip')) execFileSync('unzip', ['-q', archive, '-d', extraction]);
else execFileSync('tar', ['-xzf', archive, '-C', extraction]);
const runtimeBinary = path.join(core, 'runtime', target.startsWith('win') ? 'node.exe' : 'node');
// Do not overwrite the inode of a runtime used by an open development client.
// In-place writes to a running Mach-O can invalidate macOS' executable cache.
const runtimeStaging = runtimeBinary + '.staging-' + process.pid;
fs.copyFileSync(path.join(extraction, runtime.root, runtime.binary), runtimeStaging);
fs.chmodSync(runtimeStaging, 0o755);
fs.renameSync(runtimeStaging, runtimeBinary);
fs.copyFileSync(path.join(extraction, runtime.root, 'LICENSE'), path.join(core, 'runtime', 'LICENSE'));
fs.writeFileSync(path.join(core, 'runtime', 'SOURCE.json'), JSON.stringify(runtime, null, 2));
await build({ entryPoints: [path.join(root, 'src/client.ts')], bundle: true, platform: 'node', format: 'esm', target: 'node18', outfile: path.join(core, 'client.mjs'),
  banner: { js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);" } });
fs.copyFileSync(path.join(root, 'desktop/worker.mjs'), path.join(core, 'worker.mjs'));
for (const file of ['client.html', 'icon-192.png', 'icon-512.png']) fs.copyFileSync(path.join(root, 'static', file), path.join(core, 'static', file));
fs.copyFileSync(path.join(root, 'node_modules/ws/LICENSE'), path.join(core, 'WS-LICENSE'));
if (prepareOnly) { console.log('Prepared desktop resources:', core); process.exit(0); }
const output = path.join(root, 'dist/desktop', target);
fs.mkdirSync(output, { recursive: true });
if (target === 'linux-x64-web') {
  fs.writeFileSync(path.join(core, 'start.sh'), '#!/bin/sh\ncd "$(dirname "$0")"\nexec ./runtime/node ./client.mjs "$@"\n', { mode: 0o755 });
  fs.writeFileSync(path.join(core, 'README.txt'), 'YwMatrix Linux x64\nRun ./start.sh and open http://127.0.0.1:9321\nConfiguration: ~/.agent-manage/connector.json\n');
  execFileSync('tar', [...archiveFlags, '-czf', path.join(output, `YwMatrix-${appVersion}-linux-x64-web.tar.gz`), '-C', path.dirname(core), 'core'], { env: { ...process.env, COPYFILE_DISABLE: '1' } });
} else {
  const require = createRequire(import.meta.url);
  const { build: packageApp, Platform, Arch } = require('../desktop/node_modules/electron-builder');
  const config = require('../desktop/electron-builder.cjs');
  config.directories.output = output;
  config.extraResources = [{ from: core, to: 'core' }];
  config.electronVersion = target === 'win7-x64' ? '22.3.27' : '44.4.3';
  if (target === 'win7-x64') config.nsis.artifactName = 'YwMatrix-Setup-${version}-win7-${arch}.exe';
  const platform = target.startsWith('win') ? Platform.WINDOWS : target.startsWith('mac') ? Platform.MAC : Platform.LINUX;
  const arch = target.endsWith('arm64') ? Arch.arm64 : Arch.x64;
  const targets = platform.createTarget(target.startsWith('win') ? 'nsis' : target.startsWith('mac') ? 'dmg' : 'dir', arch);
  await packageApp({ projectDir: path.join(root, 'desktop'), config, targets });
  if (target === 'linux-arm64') {
    const unpacked = path.join(output, 'linux-arm64-unpacked');
    fs.writeFileSync(path.join(unpacked, 'start.sh'), '#!/bin/sh\ncd "$(dirname "$0")"\nexec ./ywmatrix "$@"\n', { mode: 0o755 });
    fs.writeFileSync(path.join(unpacked, 'install-shortcut.sh'), '#!/bin/sh\nset -eu\napp_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\nmenu_dir="${XDG_DATA_HOME:-$HOME/.local/share}/applications"\nmkdir -p "$menu_dir"\nprintf \'[Desktop Entry]\\nType=Application\\nName=YwMatrix\\nExec="%s/start.sh"\\nIcon=%s/resources/core/static/icon-512.png\\nTerminal=false\\nCategories=Utility;\\n\' "$app_dir" "$app_dir" > "$menu_dir/ywmatrix.desktop"\nprintf \'YwMatrix shortcut created.\\n\'\n', { mode: 0o755 });
    fs.writeFileSync(path.join(unpacked, 'README.txt'), 'YwMatrix ARM64 desktop portable\nExtract to a writable user directory and run ./start.sh.\nRequires a compatible Linux desktop (GTK3/NSS and Chromium sandbox support).\nNo Node installation required. Data stays in ~/.agent-manage when the application is moved or upgraded.\nTarget Kylin version must be validated on hardware. Do not run as root or disable the sandbox.\n');
    execFileSync('tar', [...archiveFlags, '-czf', path.join(output, `YwMatrix-${appVersion}-linux-arm64-portable.tar.gz`), '-C', output, 'linux-arm64-unpacked'], { env: { ...process.env, COPYFILE_DISABLE: '1' } });
  }
}
const artifacts = fs.readdirSync(output).filter(name => name.includes(`-${appVersion}-`) && /\.(exe|dmg|tar\.gz)$/.test(name));
const manifest = artifacts.map(name => ({ file: name, size: fs.statSync(path.join(output, name)).size,
  sha256: crypto.createHash('sha256').update(fs.readFileSync(path.join(output, name))).digest('hex') }));
fs.writeFileSync(path.join(output, 'artifacts.json'), JSON.stringify({ version: appVersion, target, runtime: runtime.version, testedOnTargetOS: false, artifacts: manifest }, null, 2));
console.log('Built:', output);
