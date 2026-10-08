// 产品分发目录（安装包仓库）：data/products/<brand>/<version> 的扫描/校验/发布。
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import type { Db } from "../db.ts";
import { logger } from "../util.ts";
import { readTarEntry } from "../tar.ts";
import { validateTargets } from "../product-platform.ts";

// ---- 产品分发目录（安装包仓库）：data/products/<brand>/<version>/{manifest.json,package.tar.gz,meta.json} ----

export interface ProductCatalogEntry {
  brand: string;
  version: string;
  manifest: Record<string, unknown>;
  sha256: string;
  size: number;
  updated_at: number;
  artifact_id?: string;
}

export function validProductBrand(s: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(s);
}
export function validProductVersion(s: string): boolean {
  return /^(?:\d+\.){2}\d+(?:-[0-9A-Za-z.+-]+)?$/.test(s);
}

export function validateProductManifest(m: Record<string, unknown>): Record<string, unknown> {
  if (m.format !== 1) throw new Error("manifest.format 必须为 1");
  if (typeof m.brand !== "string" || !validProductBrand(m.brand)) throw new Error("manifest.brand 非法");
  if (typeof m.version !== "string" || !validProductVersion(m.version)) throw new Error("manifest.version 需要 semver（如 1.0.0）");
  if (typeof m.kind !== "string" || !["stdio", "http", "ws", "web", "app"].includes(m.kind)) throw new Error("manifest.kind 非法");
  validateTargets(m.targets);
  if (m.artifact_id !== undefined && (typeof m.artifact_id !== "string" || !validProductBrand(m.artifact_id))) throw new Error("artifact_id 非法");
  return m;
}

// 安全路径拼接：brand/version 白名单字符校验后再 join
export function productDirPath(root: string, brand: string, version: string, artifact?: string | null): string | null {
  if (!validProductBrand(brand) || !validProductVersion(version)) return null;
  if (artifact && !validProductBrand(artifact)) return null;
  return artifact ? path.join(root, brand, version, artifact) : path.join(root, brand, version);
}
export function productPackagePath(root: string, brand: string, version: string, artifact?: string | null): string | null {
  const dir = productDirPath(root, brand, version, artifact);
  return dir ? path.join(dir, "package.tar.gz") : null;
}

export interface ProductMeta { sha256: string; size: number; uploaded_at: number; }
export function readProductMeta(dir: string): ProductMeta | null {
  try {
    return JSON.parse(fs.readFileSync(path.join(dir, "meta.json"), "utf8")) as ProductMeta;
  } catch {
    return null;
  }
}

export function scanProductCatalog(root: string): ProductCatalogEntry[] {
  const out: ProductCatalogEntry[] = [];
  let brands: fs.Dirent[] = [];
  try {
    brands = fs.readdirSync(root, { withFileTypes: true }).filter(e => e.isDirectory() && !e.name.startsWith("."));
  } catch {
    return out; // 目录不存在 = 空目录
  }
  for (const b of brands) {
    let versions: fs.Dirent[] = [];
    try {
      versions = fs.readdirSync(path.join(root, b.name), { withFileTypes: true })
        .filter(e => e.isDirectory() && validProductVersion(e.name));
    } catch { continue; }
    for (const v of versions) {
      const versionDir = path.join(root, b.name, v.name);
      const directories = [versionDir, ...fs.readdirSync(versionDir, { withFileTypes: true }).filter(e => e.isDirectory() && validProductBrand(e.name)).map(e => path.join(versionDir, e.name))];
      for (const dir of directories) {
      try {
        const manifest = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8")) as Record<string, unknown>;
        const meta = readProductMeta(dir);
        if (!meta) continue;
        out.push({
          brand: b.name,
          version: v.name,
          manifest,
          sha256: meta.sha256,
          size: meta.size,
          updated_at: meta.uploaded_at,
          ...(dir !== versionDir ? { artifact_id: path.basename(dir) } : {}),
        });
      } catch { /* 坏条目跳过 */ }
      }
    }
  }
  out.sort((x, y) => x.brand.localeCompare(y.brand) || (x.updated_at - y.updated_at));
  return out;
}

// 发布：从 tar 里抽 manifest 校验后落盘（manifest.json 服务端为准，可后续编辑）+ 算 sha256 存 meta。
// 品牌=产品身份：上架前必须已存在同名品牌（先建品牌再传包）
export async function publishProductPackage(root: string, buf: Buffer, filename: string, db?: Db): Promise<{
  brand: string; version: string; sha256: string; size: number;
}> {
  const raw = readTarEntry(buf, "manifest.json");
  if (!raw) throw new Error("安装包内缺少 manifest.json（" + filename + "）");
  const manifest = validateProductManifest(JSON.parse(raw.toString("utf8")) as Record<string, unknown>);
  const brand = manifest.brand as string;
  const version = manifest.version as string;
  if (db && !(await db.getBrandByName(brand))) {
    throw new Error("品牌「" + brand + "」不存在：请先在品牌管理创建同名品牌，再上传产品包");
  }
  const dir = productDirPath(root, brand, version, manifest.artifact_id as string | undefined)!;
  if (fs.existsSync(path.join(dir, "package.tar.gz"))) {
    throw new Error("该产品版本已发布：" + brand + " " + version + "（如需覆盖请先删除）");
  }
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "package.tar.gz"), buf);
  fs.writeFileSync(path.join(dir, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
  const meta: ProductMeta = {
    sha256: crypto.createHash("sha256").update(buf).digest("hex"),
    size: buf.length,
    uploaded_at: Date.now(),
  };
  fs.writeFileSync(path.join(dir, "meta.json"), JSON.stringify(meta), "utf8");
  return { brand, version, sha256: meta.sha256, size: meta.size };
}
