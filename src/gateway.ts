// 网关入口（实现已拆分至 src/gateway/）：
//   types      共享类型/常量/chunk 纯函数
//   hub        Hub 连接注册表 + 任务跟踪/落库缓冲/编排回投
//   sessions   会话与消息 RPC
//   groups     群组管理 RPC（含 delegates 授权矩阵）
//   users      用户管理 RPC
//   agents     Agent/品牌/设备密钥/配对码/connector RPC 与注册
//   products   产品分发目录
//   orchestration  任务入口 + 管理者编排（invoke/collect/群黑板/run.list）
//   dispatch   agent/用户双通道消息路由
//   http       服务器装配（配置/登录 SSO/静态/附件/WS）
//   main       进程入口（jwt secret/DB 初始化/优雅关闭）
export { createGatewayServer, loadGatewayConfig, type GatewayConfig } from "./gateway/http.ts";
export { Hub } from "./gateway/hub.ts";
export { handleUserMessage, handleAgentMessage } from "./gateway/dispatch.ts";

import path from "node:path";
import { fileURLToPath } from "node:url";
import { main, rotateJwtSecret } from "./gateway/main.ts";

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  if (process.argv.includes("--rotate-secret") || process.argv.includes("-rotate-secret")) {
    void rotateJwtSecret().then(() => process.exit(0));
  } else {
    // 入口在本文件：静态资源相对入口目录解析（与拆分前行为一致，bundle 部署同样成立）
    const staticFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "static", "index.html");
    void main(staticFile);
  }
}
