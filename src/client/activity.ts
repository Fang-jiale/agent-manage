import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export interface ClientActivity {
  id: string;
  title: string;
  status: "running" | "done" | "failed";
  stage: string;
  created_at: number;
  updated_at: number;
  error?: string;
}

export class ActivityStore {
  private items: ClientActivity[] = [];
  private filename: string;
  constructor(filename: string) {
    this.filename = filename;
    try {
      const parsed = JSON.parse(fs.readFileSync(filename, "utf8"));
      if (Array.isArray(parsed)) this.items = parsed.slice(0, 100);
    } catch { /* first launch */ }
    for (const item of this.items) if (item.status === "running") {
      item.status = "failed";
      item.stage = "客户端重启，操作未确认完成";
    }
  }
  list(): ClientActivity[] { return this.items.map(item => ({ ...item })); }
  start(title: string): string {
    const id = crypto.randomUUID();
    this.items.unshift({ id, title, status: "running", stage: "准备中", created_at: Date.now(), updated_at: Date.now() });
    this.items = this.items.slice(0, 100);
    this.save();
    return id;
  }
  update(id: string, stage: string, status: ClientActivity["status"] = "running", error?: string): void {
    const item = this.items.find(item => item.id === id);
    if (!item) return;
    Object.assign(item, { stage, status, updated_at: Date.now(), ...(error ? { error } : {}) });
    this.save();
  }
  private save(): void {
    fs.mkdirSync(path.dirname(this.filename), { recursive: true });
    const staging = this.filename + ".tmp";
    fs.writeFileSync(staging, JSON.stringify(this.items), { mode: 0o600 });
    fs.renameSync(staging, this.filename);
  }
}
