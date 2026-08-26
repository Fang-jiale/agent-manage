# claude-bridge:把真·Claude Code 接入 YwMatrix

shim(`ywmatrix-shim.mjs`)按"自己所在目录找 `cli.mjs`"的方式拉起 CLI——
本目录放它的**副本** + 一个 3 行转发器 `cli.mjs`(把同样的参数原样转给 PATH 里的
`claude`),即可让真 Claude Code 说本地 Agent 协议,原 `package/dist` 的 ywcoder
不受影响。

## 部署步骤(每台机器一次)

```bash
cp package/dist/ywmatrix-shim.mjs package/claude-bridge/     # shim 副本(勿提交,16MB)
# 模型端点:~/.claude/settings.json 的 env 里配 ANTHROPIC_BASE_URL/AUTH_TOKEN
#   (例:智谱 GLM 的 Anthropic 兼容端点 https://open.bigmodel.cn/api/anthropic)
# shim 的依赖解析走上级目录:需保证 package/node_modules 存在(从开发机整目录拷,
#   npm 单装 @opentelemetry/* 会互相卸载,拷目录才稳)
```

品牌 launch_cmd 示例:

```
node <项目>/package/claude-bridge/ywmatrix-shim.mjs --workdir <目录> --permission-mode default
```
