# 工行 AAM 统一认证接入

## 流程（对接文档 3.1/3.2 标准用法）

```
登录页「使用统一认证登录」
→ 网关空参数调桥：SDK 内部 sendRedirect 到 AAM 授权页（地址由 SDK 构造，无需配置）
→ 用户凭办公软件状态自动放行 → 带 ssiAuth/ssiSign 跳回 /auth/aam/callback
→ 网关带参再调桥：
   new ServerSideAuthenticator() + 六 setter 初始化
   → setOperation("signIn") → execute(req, resp, ssiAuth, ssiSign)（桥内构造假 servlet 对象）
   → 从 request 属性 ssiCredentials → getSSICUser() → employeeNo（工号）
→ 按工号自动建号 + 签发 JWT（账号密码登录保留兜底）
验签失败但 SDK 指了重登地址（会话失效等）→ 网关带浏览器 302 重走登录
```

## 组成

- `AamBridge.java` —— SDK 桥（反射调用 `com.icbc.ssic.base.ServerSideAuthenticator`，**编译时不需要 jar**；假 servlet 对象用动态代理实现，未知方法调用会打 stderr 日志便于排查）
- `src/aam.ts` —— 网关侧适配（跳转 / ssiAuth+ssiSign 验签 / \r\n 剥离 / 建号）
- `aam-sm-2.0.jar` —— 官方 SDK（社区下载，放本目录，不入 git）
- 可能还需要 `servlet-api.jar`（若 SDK jar 不自带 javax.servlet 类；放本目录即可）

## 部署步骤（网关所在机器，需 JDK）

```bash
# 1. SDK jar（及如需的 servlet-api.jar）放入 aam/ 目录
cp ~/aam-sm-2.0.jar /opt/ywmatrix/aam/

# 2. 编译桥（无需 jar 在场）
cd /opt/ywmatrix/aam && javac AamBridge.java

# 3. systemd unit 的 [Service] 段加（五项核心全配才启用；跳转地址 SDK 内部构造，无需配置）：
#    Environment=AGENT_MANAGE_AAM_SERVER=aam.icbc
#    Environment=AGENT_MANAGE_AAM_SERVICE_NAME=你的应用标识
#    Environment=AGENT_MANAGE_AAM_SM_PUBLIC_KEY=<注册时发的SM2公钥JSON>
#    Environment=AGENT_MANAGE_AAM_SM_KEY_PASS=<公钥保护口令>
#    Environment=AGENT_MANAGE_AAM_BRIDGE_CMD=java -cp /opt/ywmatrix/aam:/opt/ywmatrix/aam/* AamBridge
#    可选：AGENT_MANAGE_AAM_SERVICE_URL（显式指定回调地址，缺省按请求 Host 推导；
#          必须与注册到 AAM 的回调完全一致——协议/域名/路径）
#          AGENT_MANAGE_AAM_VERSION（默认 SM2）
systemctl daemon-reload && systemctl restart ywmatrix-gateway

# 4. 登录页出现「使用统一认证登录」即接入完成
```

源码部署（/opt/agent-manage）同理，替换路径即可。

## 排错

- 手工单测桥（不经网关，直接看 SDK 反应）：
  `echo '{"serverName":"aam.icbc","serviceName":"x","serviceURL":"https://gw/auth/aam/callback","smPublicKey":"{...}","smKeyPass":"...","ssiAuth":"...","ssiSign":"..."}' | java -cp .:aam-sm-2.0.jar AamBridge`
- 类找不到：`jar tf aam-sm-2.0.jar | grep -i authenticator` 看实际类名，必要时 `BRIDGE_CMD` 加 `-Daam.auth.class=完整类名`
- 报 "classpath 缺 servlet API"：放一个 servlet-api.jar 进 aam/ 目录
- 验签失败但参数确定没错：检查 `SERVICE_URL` 与注册到 AAM 的回调地址是否**完全一致**（协议/域名/路径）
- 桥的 stderr 有每个被调用的假 servlet 方法日志，首次实测时 `journalctl -u ywmatrix-gateway` 看它还需要什么

## 安全说明

- ssiAuth/ssiSign 只经 stdin 传给桥进程（不进命令行，ps 不可见）
- 验签（SM2）由官方 SDK 完成，网关不接触密码
- 工号按 `employee_id` 关联账号，首次登录自动建号（随机密码占位，本地密码登录不可用），与 OIDC 建号逻辑共用
