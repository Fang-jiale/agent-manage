export interface ProductTarget { os: string; arch: string; min_os?: string; max_os?: string; }
const operatingSystems = new Set(["win32", "linux", "darwin"]);
const architectures = new Set(["x64", "arm64", "ia32"]);
export function validateTargets(value: unknown): ProductTarget[] | undefined {
  if (value === undefined) return;
  if (!Array.isArray(value) || !value.length) throw new Error("targets 需要至少一个系统与架构组合");
  return value.map(target => {
    if (!target || !operatingSystems.has(target.os) || !architectures.has(target.arch)) throw new Error("非法的产品系统或架构");
    for (const key of ["min_os", "max_os"]) if (target[key] !== undefined && (typeof target[key] !== "string" || !/^\d+(?:\.\d+){0,3}$/.test(target[key]))) throw new Error("非法系统版本限制");
    return target as ProductTarget;
  });
}
function compare(a: string, b: string): number {
  const left = a.split('.').map(Number), right = b.split('.').map(Number);
  for (let i = 0; i < Math.max(left.length, right.length); i++) {
    if ((left[i] || 0) !== (right[i] || 0)) return (left[i] || 0) - (right[i] || 0);
  }
  return 0;
}
export function productCompatible(manifest: { targets?: unknown }, os: string, arch: string, version: string): boolean {
  const targets = validateTargets(manifest.targets);
  if (!targets) return true; // legacy packages remain explicit/unclassified in the UI
  return targets.some(target => target.os === os && target.arch === arch &&
    (!target.min_os || compare(version, target.min_os) >= 0) &&
    (!target.max_os || compare(version.split('.').slice(0, target.max_os.split('.').length).join('.'), target.max_os) <= 0));
}
