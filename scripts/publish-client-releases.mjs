import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = path.join(root, 'dist/desktop');
const destination = path.resolve(process.argv[2] || path.join(root, 'data/client-releases'));
const labels = { 'win-x64': 'Windows 10/11 · 64 位安装版', 'win7-x64': 'Windows 7 · 64 位兼容版', 'linux-arm64': '麒麟 ARM64 · 解压版', 'linux-x64-web': 'Linux x64 · Web 管理版', 'mac-arm64': 'macOS · Apple 芯片' };
fs.mkdirSync(destination, { recursive: true });
const releases = [];
for (const target of Object.keys(labels)) {
  const manifestPath = path.join(source, target, 'artifacts.json');
  if (!fs.existsSync(manifestPath)) continue;
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  for (const item of manifest.artifacts) {
    if (!/^[A-Za-z0-9._-]+$/.test(item.file)) throw Error('Invalid artifact filename');
    const file = path.join(source, target, item.file);
    const digest = crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
    if (digest !== item.sha256) throw Error('Artifact checksum mismatch: ' + item.file);
    fs.copyFileSync(file, path.join(destination, item.file));
    releases.push({ ...item, target, label: labels[target], version: manifest.version, tested: manifest.testedOnTargetOS === true });
  }
}
fs.writeFileSync(path.join(destination, 'index.json.tmp'), JSON.stringify({ releases }, null, 2));
fs.renameSync(path.join(destination, 'index.json.tmp'), path.join(destination, 'index.json'));
console.log(`Published ${releases.length} client artifacts to ${destination}`);
