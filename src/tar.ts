// 共享 tar.gz 工具：安装器两端（client 解包安装 / gateway 上传时读 manifest）共用。
// 最小 ustar 读取器：文件与目录、GNU 长名(L)、pax 头跳过；条目路径做穿越防护。
import zlib from "node:zlib";
import fs from "node:fs";
import path from "node:path";

interface TarEntry {
  name: string;
  type: string;
  data: Buffer;
}

function str(b: Buffer, start: number, len: number): string {
  const slice = b.subarray(start, start + len);
  const end = slice.indexOf(0);
  return slice.subarray(0, end === -1 ? len : end).toString("utf8").trim();
}

function parseTar(buf: Buffer): TarEntry[] {
  const entries: TarEntry[] = [];
  let off = 0;
  let longName: string | null = null;
  while (off + 512 <= buf.length) {
    const header = buf.subarray(off, off + 512);
    if (header.every(v => v === 0)) break; // 结束全零块
    const nameField = str(header, 0, 100);
    const size = parseInt(str(header, 124, 12), 8) || 0;
    const type = String.fromCharCode(header[156] || 0x30);
    const prefix = str(header, 345, 155);
    const name = longName ?? (prefix ? prefix + "/" + nameField : nameField);
    longName = null;
    const dataStart = off + 512;
    const data = buf.subarray(dataStart, dataStart + size);
    off = dataStart + Math.ceil(size / 512) * 512;
    if (type === "L") { // GNU 长文件名：内容是下一条目的路径
      longName = data.toString("utf8").replace(/\0+$/, "");
      continue;
    }
    if (type === "x" || type === "g") continue; // pax 扩展头：跳过
    entries.push({ name, type, data });
  }
  return entries;
}

// 从 tar.gz 里按名取单个文件（如 manifest.json）；兼容 "./" 前缀。找不到返回 null。
export function readTarEntry(gzBuf: Buffer, wanted: string): Buffer | null {
  const tar = zlib.gunzipSync(gzBuf);
  const target = wanted.replace(/^\.\//, "").replace(/\/+$/, "");
  for (const e of parseTar(tar)) {
    if (e.type !== "0" && e.type !== "\0") continue;
    const name = e.name.replace(/^\.\//, "").replace(/\/+$/, "");
    if (name === target) return e.data;
  }
  return null;
}

// 安全解包到目标目录；返回写入的文件数。拒绝 .. 与绝对路径条目。
export function extractTarGz(gzBuf: Buffer, dest: string): number {
  const tar = zlib.gunzipSync(gzBuf);
  let files = 0;
  for (const e of parseTar(tar)) {
    if (e.type !== "0" && e.type !== "\0" && e.type !== "5") continue; // 链接等不支持
    const rel = e.name.replace(/^\.?\//, "").replace(/\/+$/, "");
    if (rel === "" || rel === ".") continue; // 根目录条目（./ 或 ./dir/ 已归一化）
    if (rel.includes("..") || path.isAbsolute(rel)) {
      throw new Error("unsafe entry in archive: " + e.name);
    }
    const target = path.join(dest, rel);
    if (!target.startsWith(dest + path.sep)) throw new Error("unsafe entry in archive: " + e.name);
    if (e.type === "5") {
      fs.mkdirSync(target, { recursive: true });
      continue;
    }
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, e.data);
    fs.chmodSync(target, 0o755); // 产物里的可执行位不依赖打包机
    files++;
  }
  if (files === 0) throw new Error("archive contains no files");
  return files;
}
