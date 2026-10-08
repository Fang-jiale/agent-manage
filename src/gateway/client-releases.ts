import fs from "node:fs";
import path from "node:path";
export interface ClientRelease { file: string; label: string; version: string; size: number; sha256: string; tested: boolean; target: string; }
export function listClientReleases(directory: string): ClientRelease[] {
  try {
    const value = JSON.parse(fs.readFileSync(path.join(directory, "index.json"), "utf8"));
    if (!Array.isArray(value.releases)) return [];
    return value.releases.filter((entry: ClientRelease) => typeof entry.file === "string" && /^[A-Za-z0-9._-]+\.(exe|dmg|tar\.gz)$/.test(entry.file) &&
      typeof entry.label === "string" && typeof entry.version === "string" && /^[a-f0-9]{64}$/.test(entry.sha256) &&
      fs.existsSync(path.join(directory, entry.file)) && fs.statSync(path.join(directory, entry.file)).size === entry.size);
  } catch { return []; }
}
