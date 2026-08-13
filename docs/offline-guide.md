# YwMatrix 离线包部署与使用手册

适用版本：含品牌治理 / 配对接入 / 三连接方式（stdio·http·ws）的离线包。
打包方法（源码侧）：`npm run bundle && node scripts/build-offline.mjs`，产物在 `dist/offline/packages/`。

## 1. 包清单

| 包 | 用途 |
|----|------|
| `ywmatrix-server-linux-x64.tar.gz` | 网关服务端（Linux x86_64，自带 Node 18 运行时） |
| `ywmatrix-client-linux-x64.tar.gz` | 终端 AgentClient（Linux x86_64） |
| `ywmatrix-client-linux-arm64.tar.gz` | 终端 AgentClient（Linux arm64：麒麟/统信、飞腾/鲲鹏） |
| `ywmatrix-client-win-x64.zip` | 终端 AgentClient（Windows x64） |

所有包自带 Node 运行时，目标机器零依赖。网关另需 MySQL 8.0+（单机部署无需 Redis）。

## 2. 服务端部署（Linux）

```bash
sudo mkdir -p /opt/ywmatrix
sudo tar xzf ywmatrix-server-linux-x64.tar.gz -C /opt/ywmatrix --strip-components=1
sudo useradd -r -s /sbin/nologin ywmatrix
sudo mkdir -p /opt/ywmatrix/data/attachments
sudo chown -R ywmatrix:ywmatrix /opt/ywmatrix

# MySQL 只需建库建号（表结构网关自动建，见下）：
#   CREATE DATABASE IF NOT EXISTS ywmatrix CHARACTER SET utf8mb4;
#   CREATE USER IF NOT EXISTS 'ywmatrix'@'%' IDENTIFIED BY '强密码';
#   GRANT ALL PRIVILEGES ON ywmatrix.* TO 'ywmatrix'@'%';

sudo cp /opt/ywmatrix/ywmatrix-gateway.service /etc/systemd/system/
sudo vi /etc/systemd/system/ywmatrix-gateway.service   # 替换 2 处 CHANGE_ME：DB 密码、admin 初始密码
sudo systemctl daemon-reload && sudo systemctl enable --now ywmatrix-gateway
curl http://127.0.0.1:8080/healthz   # {"status":"ok"} 即就绪
```

访问 `http://服务器IP:8080`（聊天页）与 `http://服务器IP:8080/admin`（管控台），admin + 初始密码登录。

### 2.1 升级与数据库迁移（不需要手工 SQL）

**所有表结构变更由网关启动时自动完成，幂等可反复执行，不需要单独跑 SQL：**

- 首启自动建全部表：`users / sessions / messages / agents / device_keys / agent_brands / pairing_codes`；
- 老库升级自动补列：`agents.brand_id / connector_id / approval_status`，`agent_brands.launch_cmd / conn_type / endpoint`（ALTER 已存在则跳过）。

升级操作：停服 → 覆盖 `bin/` 和 `static/` → 启服。建议升级前 `mysqldump` 备份。

### 2.2 品牌治理模式（行为变化，务必知晓）

- **不建品牌 = 开放模式**：agent 自由注册，与旧版行为一致，老 client 不受影响。
- **建了品牌即进入治理模式**：注册必须带合法 `brand_id`，自由注册的老 client 会被拒并断线；client 主动注册进入「待审批」。
- 推荐做法：先建品牌目录，再让终端全部走 connector 配对接入（见下），agent 实例统一在页面上分配。

### 2.3 生产建议

- 前面加 Nginx/Caddy 反代终结 TLS：放行 WebSocket Upgrade、`client_max_body_size 32m`、`proxy_read_timeout 3600s`（参考 `deploy/` 现成配置）。
- 防火墙只需放行终端 → 服务器 8080 入站；终端侧全部 outbound，不开端口。
- 运维端点：`GET /healthz`、`GET /metrics`。

## 3. 终端接入（三平台 AgentClient）

以 Linux 为例（Windows 对应 `start.bat`）：

```bash
tar xzf ywmatrix-client-linux-*.tar.gz && cd ywmatrix-client-linux-*
chmod +x start.sh runtime/node
```

**首次接入（配对，一次性）：**

1. 管理员在管控台「设备密钥 → 生成配对码」，把码发给终端用户；
2. 终端编辑 `start.sh` 填 `GATEWAY` 和 `PAIR_CODE`，执行 `./start.sh`；
3. 管理员在管控台「Agent 管理 → 待接入」点**批准**——批准后终端自动完成接入，凭证写入 `~/.agent-manage/connector.json`（0600）。

> 图形化替代：直接 `./runtime/node ./client.mjs`，浏览器打开终端本机 `http://127.0.0.1:9321`，页面上填网关地址 + 配对码。

**之后每次启动**：`./start.sh` 零参数。

**配对码归属**：谁生成的码，该终端注册的所有 agent 就归谁（页面可见性/消息按属主隔离）；admin 可代他人生成。码一次性、默认 24h 有效。

**Windows 开机自启**：任务计划程序建"登录时运行"任务指向 `start.bat`；Linux 用包内 `ywmatrix-client.service`（先手工完成配对再 enable，service 的 `User` 必须是配对时用的账号）。

## 4. 日常使用手册

### 4.1 管理员：管控台（/admin）

- **品牌管理**：维护 agent 目录——名称、logo、能力标签、**连接方式**（stdio 命令启动 / http 服务地址 / ws 服务地址）及对应命令或地址。品牌是终端可选的唯一类型来源。
- **设备密钥**：长期凭证（`amk_`）的手工管理（推荐用配对码代替）；**生成配对码**给新终端接入用。
- **Agent 管理**：
  - 「待接入」批准/拒绝新 connector 的配对请求；
  - 「注册 Agent」给在线 connector 分配品牌实例（也可由终端在本地页自助分配）；
  - client 自由注册的 agent 显示「待审批」，批准后才可接任务；
  - 移除 connector 托管实例用「移除」（不要用断开，实例共享连接）。
- **用户**：账号、角色、禁用、重置密码。

### 4.2 终端用户：本机管理页（http://127.0.0.1:9321）

- 未接入：页面完成配对（网关地址 + 配对码）；
- 已接入：查看本机托管 agent（品牌/连接方式/运行状态/生效命令或地址）、**添加 agent**（从品牌目录选类型，可填本机覆盖命令/地址）、**编辑覆盖**（如本机路径与品牌默认不同）、**移除**。
- 覆盖优先级：本机覆盖 > 品牌默认 > client 启动参数兜底。

### 4.3 最终用户：聊天页（/）

左侧选 agent（按属主隔离，只能看到自己的；admin 可见全部），右侧对话。工具确认框随任务流式弹出，审批不设超时。

## 5. FAQ

- **升级后要跑 SQL 吗？** 不用，全部自动迁移，见 §2.1。
- **老 client 升级后连不上？** 检查是否建了品牌（治理模式）。要么先不建品牌，要么老终端改走配对接入。
- **配对码报错 invalid or expired？** 码一次性且默认 24h 有效，重新生成。
- **换网关 / 重新接入？** 删 `~/.agent-manage\connector.json`（Windows 同路径用户目录下）重启后重新配对。
- **终端能自己注册任意 agent 吗？** 治理模式下不能：只能在品牌目录里选，且主动注册需审批。
- **本地页安全吗？** 只绑 127.0.0.1，无远程访问；凭证文件 0600。
