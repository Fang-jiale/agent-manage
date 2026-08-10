// 附件存储：默认本地盘（网关回源 /files/*），配置 s3-endpoint 后切换为
// S3 兼容对象存储（MinIO / AWS S3 / OSS）。两者都不配置时网关不启用
// 附件上传（页面退化为消息内嵌 base64）。

import fs from "node:fs/promises";
import path from "node:path";
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  CreateBucketCommand,
  PutBucketPolicyCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";
import { logger } from "./util.ts";

export interface AttachmentStore {
  put(key: string, body: Buffer, mime: string): Promise<string>;
  delete(key: string): Promise<void>;
  // 从附件 URL 反推存储 key；非本存储产生的 URL 返回 undefined
  keyFromUrl(url: string): string | undefined;
}

// ---- 本地盘 ----

const EXT_MIME: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".pdf": "application/pdf",
  ".txt": "text/plain; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
  ".json": "application/json",
  ".zip": "application/zip",
};

export class LocalAttachmentStore implements AttachmentStore {
  private dir: string;
  private urlBase: string;

  constructor(dir: string, urlBase = "/files") {
    this.dir = dir;
    this.urlBase = urlBase;
  }

  async init(): Promise<void> {
    await fs.mkdir(this.dir, { recursive: true });
  }

  async put(key: string, body: Buffer, _mime: string): Promise<string> {
    const file = this.fileFor(key);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, body);
    return `${this.urlBase}/${key}`;
  }

  async get(key: string): Promise<{ body: Buffer; mime: string } | undefined> {
    try {
      const body = await fs.readFile(this.fileFor(key));
      const mime = EXT_MIME[path.extname(key).toLowerCase()] ?? "application/octet-stream";
      return { body, mime };
    } catch {
      return undefined;
    }
  }

  async delete(key: string): Promise<void> {
    await fs.rm(this.fileFor(key), { force: true });
    // 顺手清理空目录（最多向上两级：uuid 目录与用户目录）
    for (let d = path.dirname(this.fileFor(key)), i = 0; i < 2 && d.length > this.dir.length; d = path.dirname(d), i++) {
      await fs.rmdir(d).catch(() => {});
    }
  }

  keyFromUrl(url: string): string | undefined {
    const prefix = `${this.urlBase}/`;
    return url.startsWith(prefix) ? url.slice(prefix.length) : undefined;
  }

  // 统计某个 key 前缀（如 attachments/<uid>）下的总字节数，用于配额
  async usage(prefix: string): Promise<number> {
    let total = 0;
    const walk = async (dir: string): Promise<void> => {
      const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
      for (const e of entries) {
        const p = path.join(dir, e.name);
        if (e.isDirectory()) await walk(p);
        else total += (await fs.stat(p).catch(() => undefined))?.size ?? 0;
      }
    };
    await walk(this.fileFor(prefix));
    return total;
  }

  private fileFor(key: string): string {
    if (key.split("/").includes("..")) throw new Error("invalid key");
    return path.join(this.dir, key);
  }
}

export async function createLocalAttachmentStore(dir: string): Promise<LocalAttachmentStore | undefined> {
  const store = new LocalAttachmentStore(dir);
  try {
    await store.init();
    return store;
  } catch (e) {
    logger.error("local attachment dir not writable, upload disabled", { dir, error: String(e) });
    return undefined;
  }
}

// ---- S3 兼容对象存储 ----

export interface S3Config {
  endpoint: string;
  region: string;
  bucket: string;
  accessKey: string;
  secretKey: string;
  publicURLBase?: string; // 对外访问前缀（缺省用 endpoint/bucket）
}

export class S3AttachmentStore implements AttachmentStore {
  private client: S3Client;
  private cfg: S3Config;

  constructor(cfg: S3Config) {
    this.cfg = cfg;
    this.client = new S3Client({
      endpoint: cfg.endpoint,
      region: cfg.region,
      credentials: { accessKeyId: cfg.accessKey, secretAccessKey: cfg.secretKey },
      forcePathStyle: true, // MinIO 需要 path-style
    });
  }

  // 确保 bucket 存在且匿名可读（消息里的 <img> 直接引用 URL）
  async init(): Promise<void> {
    try {
      await this.client.send(new HeadBucketCommand({ Bucket: this.cfg.bucket }));
    } catch {
      await this.client.send(new CreateBucketCommand({ Bucket: this.cfg.bucket }));
    }
    const policy = JSON.stringify({
      Version: "2012-10-17",
      Statement: [{
        Effect: "Allow",
        Principal: "*",
        Action: ["s3:GetObject"],
        Resource: [`arn:aws:s3:::${this.cfg.bucket}/attachments/*`],
      }],
    });
    await this.client.send(new PutBucketPolicyCommand({ Bucket: this.cfg.bucket, Policy: policy }));
  }

  urlFor(key: string): string {
    const base = this.cfg.publicURLBase ?? `${this.cfg.endpoint}/${this.cfg.bucket}`;
    return `${base}/${key}`;
  }

  async put(key: string, body: Buffer, mime: string): Promise<string> {
    await this.client.send(new PutObjectCommand({
      Bucket: this.cfg.bucket,
      Key: key,
      Body: body,
      ContentType: mime,
    }));
    return this.urlFor(key);
  }

  async delete(key: string): Promise<void> {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.cfg.bucket, Key: key }));
  }

  keyFromUrl(url: string): string | undefined {
    const prefix = `${this.urlFor("")}`;
    return url.startsWith(prefix) ? url.slice(prefix.length) : undefined;
  }
}

export async function createS3AttachmentStore(cfg: S3Config): Promise<S3AttachmentStore | undefined> {
  const store = new S3AttachmentStore(cfg);
  try {
    await store.init();
    return store;
  } catch (e) {
    logger.error("s3 init failed, attachment upload disabled", { error: String(e) });
    return undefined;
  }
}

export function sanitizeFileName(name: string): string {
  const cleaned = name.replace(/[^\w.\-一-鿿]+/g, "_");
  return cleaned.length > 80 ? cleaned.slice(-80) : cleaned || "file";
}
