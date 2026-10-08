# YwMatrix 桌面客户端

首版实现使用同一个客户端核心与界面：Linux x64 保留本机 Web；Windows 使用 Setup；麒麟 ARM64 使用桌面解压包。

## 安装包

| 目标 | 文件 | 运行时 |
| --- | --- | --- |
| Windows 10/11 x64 | `YwMatrix-Setup-1.1.2-x64.exe` | Electron 44.4.3 + Node 22.23.2 |
| Windows 7 x64 | `YwMatrix-Setup-1.1.2-win7-x64.exe` | Electron 22.3.27 + 社区 Win7 Node 18.20.2 |
| 麒麟桌面 ARM64 | `YwMatrix-1.1.2-linux-arm64-portable.tar.gz` | Electron 44.4.3 + Node 22.23.2 |
| Linux x64 Web | `YwMatrix-1.1.2-linux-x64-web.tar.gz` | Node 22.23.2；没有桌面壳 |

输出目录：`dist/desktop/<target>/`，同目录的 `artifacts.json` 包含大小与 SHA-256。
当前没有发布者代码签名；Win7、Windows 与麒麟的安装、驱动、证书及系统依赖仍需要目标系统验收，因此构建清单默认 `testedOnTargetOS: false`。
麒麟包内的 Electron 二进制已检查为 AArch64，但这不代替指定麒麟系统上的运行验证。

## 用户使用

- Windows：运行对应 Setup，按向导完成当前用户安装，打开 YwMatrix。
- 麒麟：解压后运行 `./start.sh`；可运行 `./install-shortcut.sh` 创建应用菜单入口。应用需正常桌面会话和 Chromium 沙箱支持，不以 root 运行、不默认关闭沙箱。
- Linux x64：解压后进入包内 `core` 目录运行 `./start.sh`，打开 `http://127.0.0.1:9321`。
- 首次启动：填写完整组织地址（例如 `https://agentmatrix.top:8443`）和配对码。保留管理员提供的端口；提交后可看到等待批准状态，批准后自动接入。
- 更换组织：设置中操作，旧配置保存为 `connector.json.previous`，产品文件保留；运行中任务或安装未完成时拒绝切换。
- 配置位于 `~/.agent-manage/connector.json`，安装目录默认 `~/.agent-manage/products`，活动记录位于配置目录下 `activity.json`。
- 桌面窗口关闭后可继续后台运行，托盘菜单的「退出并停止智能体」才结束后台进程。没有托盘时，窗口正常关闭退出。
- Windows/macOS 的登录自启可在设置中启用；Linux 可通过桌面环境的自启设置添加启动器。

## 构建

构建机使用 Node 24+；终端用户不需要预装 Node。运行时来源和校验值固定在 `runtime-lock.json`。

```bash
npm ci
npm ci --prefix desktop --ignore-scripts
node scripts/build-desktop.mjs win7-x64
node scripts/build-desktop.mjs win-x64
node scripts/build-desktop.mjs linux-arm64
node scripts/build-desktop.mjs linux-x64-web
```

下载失败或速度持续过低时，官方 Node 归档可回退到 npm mirror；仍必须匹配同一固定 SHA-256。Win7 社区包使用固定 GitHub release 与已记录哈希，哈希只锁定制品，不代表上游提供了签名。

首次下载需要网络，缓存位于 `dist/desktop-runtime/`。标准版和 Win7 版不共用 Electron 版本。构建工具的依赖通过 `desktop/package-lock.json` 锁定。

当前 Mac ARM64 上预览原生壳：

```bash
node scripts/build-desktop.mjs mac-arm64 --prepare-only
node desktop/node_modules/electron/install.js
npm --prefix desktop start
```

`AGENT_MANAGE_CONFIG` 可指定独立的测试配置文件；`YWM_DESKTOP_CORE` 可指定准备好的核心资源目录。桌面进程自动启动私有随机回环端口，通过 preload 的受限 IPC 调用，并使用每次启动生成的随机会话令牌，界面不持有令牌。

## 网关发布安装包

```bash
node scripts/publish-client-releases.mjs
```

此命令把已构建并校验的包复制到本地 `data/client-releases/`，生成 `index.json`。部署时将该目录上传到网关的 products 目录同级，例如 `/opt/agent-manage/data/client-releases/`。
网关提供 `/clients` 下载页；管理后台「接入设备」把下载、生成配对码和审批入口串起来。
发布元数据的 `tested` 只有经过对应系统验收后才能设为 true，构建成功不自动通过系统验收。

## 智能体产品多平台制品

同一个品牌与版本现在可以发布多个 `artifact_id`，例如：

```json
{
  "format": 1,
  "brand": "example-agent",
  "version": "1.0.0",
  "kind": "stdio",
  "artifact_id": "win7-x64",
  "targets": [{ "os": "win32", "arch": "x64", "min_os": "6.1", "max_os": "6.1" }],
  "launch_cmd": "{{install_dir}}/example.exe"
}
```

`os`：win32/linux/darwin；`arch`：x64/arm64/ia32。`max_os: 6.1` 接受 Win7 的不同补丁 build，但拒绝 Windows 10。
新客户端安装前再次校验。旧包没有 targets 时显示「未标注平台」，由用户确认后安装。
新客户端通过 `/products/catalog?variants=1` 读取平台制品；旧客户端的目录不返回这些制品，避免它们按旧规则误选。平台制品暂由客户端主动安装；旧格式制品保留原有推送功能。

## 已实现与后续边界

已实现：五区导航、异步配对与取消、重新接入与旧配置备份、状态刷新修复、独立桌面壳、单实例、托盘、受限 IPC、后台进程退出处理、目录选择、诊断摘要、安装与接入活动记录、安装后启用、平台选择与校验、网关下载及接入入口、四类安装包构建。

还未实现：客户端自动更新、更新排队与自动回滚、完整日志查看器、网络代理配置 UI、平台制品灰度推送及逐设备进度。首版在有任务时阻止安装更新；安装完成但启用失败时会明确提示并保留已安装文件，支持再次启用。已有外部目录更新仍使用原有备份机制，不宣称自动回滚。

## 验证

```bash
npm run typecheck
node --test --test-concurrency=1 test/client-desktop.test.mjs test/gateway.test.ts test/protocol.test.ts test/composer.test.mjs
```

这些测试使用临时目录和隔离本机网关，不需要真实 MySQL。目标系统验收还应覆盖：Win7 SP1 x64、指定麒麟镜像、中文路径、非管理员安装、断网重连、安装与退出、任务取消、版本覆盖及卸载保留数据。
