# 2026-09-20 界面部署与 1.1.2 内网离线包

## 服务器部署

- 网关：`124.222.187.214`，`agent-gateway.service`，`/opt/agent-manage`。
- 入口：`https://agentmatrix.top:8443/`；管理：`/admin`；客户端：`/clients`。
- 与服务器逐文件比较后，本轮只有 8 个静态界面文件发生变化；后端源码、依赖锁文件一致。
- 发布目录：`/opt/agent-manage/releases/ui-20260920-YLSJvd/`，`previous-static/` 保存部署前完整页面，`static.sha256` 保存新页面清单。
- 发布前确认运行任务为 0，备份后覆盖静态文件并重启网关以清空页面内存缓存。
- 公网首页、管理页、下载页、app.js、admin.css、shared.css、index.css 均返回 200，响应内容 SHA-256 与本地一致。
- 网关与数据库健康，4 个原有智能体全部重连，运行任务及挂起请求为 0。
- 四个 1.1.2 客户端上传后逐个校验大小和 SHA-256，再原子切换 `data/client-releases/index.json`。公网下载目录已显示 1.1.2，四个文件均返回 200，大小和 EXE/gzip 文件头正确。旧包与 `previous-client-index.json` 保留用于回滚。

## 离线包

- 客户端从 1.1.1 更新为 1.1.2：Windows 10/11 x64、Win7 x64、麒麟 ARM64 桌面、Linux x64 Web。
- 服务端：Linux x64，自带经过 runtime-lock 校验的 Node 22.23.2、应用 bundle、静态页面、systemd 模板与 AAM 桥源码。
- `scripts/build-offline.mjs --server-only` 可只构建服务端，运行前先构建 `linux-x64-web` 以准备已校验运行时。
- Linux 归档剔除 macOS 扩展属性，避免 Linux tar 的扩展头告警。
- 套件位于 `dist/releases/1.1.2/YwMatrix-1.1.2-offline-suite/`，含安装说明、四个客户端、下载目录元数据、服务端包及 SHA256SUMS。
- 最终压缩包：`dist/releases/1.1.2/YwMatrix-1.1.2-offline-suite.zip`，约 457 MiB；ZIP 完整性及内部 12 个文件的 SHA-256 全部通过校验。外层校验文件为同名 `.zip.sha256`。
- 内网仍需准备数据库、系统库以及业务所需智能体产品；包内不包含生产数据、账号配置或 TLS 私钥。

## 验证

- 类型检查、31 项相关回归测试通过；测试需要监听本机端口，应使用 Node 24 并允许本机监听。
- 服务端包在 Linux x64 上使用随包 Node 22.23.2 运行：临时 MariaDB 11.8 数据库初始化、健康检查、初始管理员登录和页面逐文件比对通过。
- 随包客户端在独立端口启动，本机 API 和新客户端界面校验通过。
- 隔离测试进程、数据库、临时账号和文件均已清理，未使用生产数据库运行测试包。
- Windows/Win7/麒麟图形桌面仍需目标系统验收，`testedOnTargetOS` / `tested` 保持 false；Windows 包未签名。
