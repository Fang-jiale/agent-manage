# Agent 产品安装器规格（installer-spec）

适用范围：AgentClient 本机管理页「产品中心」（`static/client.html`）与安装引擎（`src/client.ts`）。
目标：把 agent 产品从「人肉装 + 手改命令」变成「选包安装 + 自动建实例 + 指针切换升级」。

## 1. 核心模型

```
Agent 产品（独立个体）              安装器（Agent 中心）                网关（可选 overlay）
├─ stdio/http/ws 服务型   ←──  安装/升级/卸载/建实例  ──→  connector 挂载 + 配对（现有机制）
├─ web 网页型（URL）           品牌目录当货架                     （web/app 型不托管，
└─ app 独立应用型（路径）       「打开」即启动                       点「打开」直接启动）
```

- **品牌 = 身份**：产品与网关品牌同名（`manifest.brand` ↔ `agent_brands.name`），机器差异由 connector 的 per-agent override 承载。
- **「启动」与「接入网关」解耦**：产品装完即用；要纳管时再「创建 Agent 实例」。

## 2. 目录布局（本机）

```
~/.agent-manage/
├── products/
│   └── <brand>/
│       ├── 1.0.0/            侧车式版本目录：升级装新目录，不原地覆盖
│       ├── 1.1.0/
│       ├── current           指针文件（内容为版本号）——原子切换点
│       └── manifest.json     当前版本 manifest 副本（列表展示用）
├── configs/                  （预留）实例参数与凭证，升级不碰
└── connector.json            网关接入凭证（现有）
```

旧版本保留策略：`current` + 最新 2 个版本目录，其余自动清理。

## 3. 安装包格式

- 载体：**tar.gz**（zip 支持后续加）。
- 包根（或唯一一级子目录）必须有 `manifest.json`：

```json
{
  "format": 1,
  "brand": "hello-agent",
  "version": "1.0.0",
  "kind": "stdio",
  "name": "示例 Agent",
  "description": "一句话描述",
  "launch_cmd": "node {{install_dir}}/bin/hello.mjs --config {{install_dir}}/hello.yml",
  "endpoint": null,
  "capabilities": [{ "type": "chat", "name": "demo", "description": "演示" }]
}
```

字段规则：

| 字段 | 必填 | 说明 |
|---|---|---|
| `format` | 是 | 当前为 `1` |
| `brand` | 是 | 品牌 slug（`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`），与网关品牌 name 对应 |
| `version` | 是 | semver（`x.y.z[-prerelease]`） |
| `kind` | 是 | `stdio` / `http` / `ws` / `web` / `app` |
| `launch_cmd` | stdio 必填 | 启动命令；`{{install_dir}}` 占位在安装时解析为本机版本目录绝对路径 |
| `endpoint` | http/ws/web/app 必填 | 服务地址 / 网页 URL / 应用路径 |
| `capabilities` | 否 | 建「创建 Agent 实例」时建议网关品牌配同样的能力标签 |

## 4. 安装流程（引擎侧）

```
1. 上传   POST /api/products/install?filename=x.tar.gz（raw body；?sha256= 可选校验和）
2. 解包   staging 目录（products/.staging-*）解 tar.gz
         - 路径穿越防护：条目名拒绝 .. 与绝对路径
         - 最小 ustar 读取器：文件/目录/GNU 长名(L)，pax 头跳过；产物统一 0755
3. 校验   manifest 合法性（format/brand/version/kind）
4. 落位   rename 到 products/<brand>/<version>/（同版本已存在 → 拒绝；低于 current → 拒绝降级）
5. 切指针 先写 manifest.json 副本，再写 current 指针（失败时指针仍指旧版）
6. 清理   删旧版本（保留 current + 最新 2 个）
```

安全要点：安装包来源（U 盘 / 网关分发）+ 可选 SHA-256 校验和；安装动作在页面有明确反馈；解包不执行任何代码（执行只发生在「创建 Agent 实例」后由 connector 按命令拉起）。

## 5. 更新（升级）

- 升级 = 装一个更高版本的包，同一引擎，无特殊路径。
- 在跑实例：stdio 型先在 Agent 列表移除/停止（现有 dropAgent），装完新版本后重新「创建 Agent 实例」（或编辑覆盖命令重新指向新 `install_dir`）。
- 回滚 = 卸载新版本后重装旧版本，或手动改 `current` 指针后重建实例。
- 防降级：`version` 低于 current 的包默认拒绝，防版本回退攻击。

## 6. 创建 Agent 实例（装完 → 纳管）

`POST /api/products/<brand>/instantiate`：

1. 在网关品牌目录找同名品牌（没有 → 提示先在管理后台创建）；
2. `agent.assign` 分配实例到本 connector；
3. 按 manifest 写本机 override：
   - stdio：`launch_cmd` 的 `{{install_dir}}` → 版本目录绝对路径；
   - http/ws：endpoint。
4. connector 下个 sync 周期自动拉起。

## 7. web / app 型产品

- 网关品牌目录直接配 `conn_type: web|app` + `endpoint`（URL / 应用路径），不必有本机安装物。
- 本机管理页「产品中心 → 网关产品目录」给「打开」按钮：
  - web → 浏览器开新标签；
  - app → 后端 spawn（失败回退 `open` / `xdg-open` / `cmd /c start`）。
- web/app 品牌不出现在「添加 Agent」下拉（不可托管），assign 后 connector 也不拉进程（标记 stopped 并注明原因）。

## 8. 本地 HTTP API 一览（127.0.0.1:9321）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/products` | 已安装产品列表（含版本与 manifest） |
| POST | `/api/products/install?filename=&sha256=` | 上传安装包（raw body） |
| DELETE | `/api/products/<brand>` | 卸载（删整个品牌目录） |
| POST | `/api/products/<brand>/instantiate` | 创建 Agent 实例并指向本机安装目录 |
| POST | `/api/launch` | `{brand_id}` → web 返回 URL / app 直接拉起 |

## 9. 演进路线（未实现项）

- zip 包支持（Windows 离线包同格式分发）
- 网关侧产品分发端点（在线源：`source: gateway`，与 U 盘离线源同 manifest 格式）
- 配置三级合并（manifest 默认值 → 本机探测 → 用户表单）与 `configs/` 实例配置
- migrate hooks（升级前备份 / 数据迁移 / 失败回滚）
- 安装钩子（claude-bridge shim 副本、koffi 原生库补丁这类步骤产品化）
- 凭证终极解法：网关侧 LLM 代理，终端零 key（manifest config_schema 标 `source: gateway`）

## 10. agent 开发侧契约（摘要，详见 docs/local-agent-interface.md）

- SIGTERM 优雅退出（§6.3.1）：停任务 → 清待决确认 → flush → 限时退出
- 配置/数据/凭证外置：从注入路径读，不写安装目录
- 打包纪律：依赖整目录自带、native 模块各平台预编译、无构建安装
