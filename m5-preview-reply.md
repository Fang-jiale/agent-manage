# ywcoder 文件/图片预览 —— 管控台回复

> 回复：`m5-preview-alignment.md`
> 回复方：管控台 / YwMatrix

针对来函 4 个问题答复如下。

## 1. 传输阈值

建议 **2 MB**（不是 1 MB）。理由：

- csv / 抽取自 docx·xlsx 的文本经常超过 1 MB，1 MB 会触发频繁降级；
- 2 MB 在历史库 `messages.content`（MEDIUMTEXT，16 MB 上限）与浏览器内存之间留足安全余量；
- 单次降级提示词义清晰即可（例：`[文件 x.csv 2.4MB 超阈值 2MB，已截断预览，原文件在终端]`）。

## 2. 网关 WebSocket 单条消息上限

- **WS 层**：`new WebSocketServer({ noServer: true })` 未显式设置 `maxPayload`，使用 `ws` 包默认值 **100 MiB**。
- **实际天花板**：`messages.content` 列为 `MEDIUMTEXT`，**16 MB 上限**。
  → 单条 `stream.chunk` 应卡在 **16 MB 以下**，2 MB 阈值已远低于此，安全。

## 3. 文本文件类型识别

**走 B**（shim 包成 `resource` 块 + `mimeType`）。

理由：

- 管控台侧**已实现** `resource` 块渲染分支（按 `mimeType` 路由），shim 走 B 对管控台零改动；
- A 方案需要管控台维护 action↔result 配对 + 扩展名推断；扩展名不可靠（`.log` 可能是 JSON、无扩展名场景无法判别）；
- `resource` 块的设计初衷就是携带 mimeType 消除歧义。

## 4. 二次渲染

管控台**确认**会做二次渲染。现状盘点：

| 内容类型 | 渲染方式 | 状态 |
|---|---|---|
| `text`（markdown） | markdown → HTML（流式相邻 chunk 合并后再渲染） | ✅ 已实现 |
| `image` | `<img>`（data:base64 或 url 双路径） | ✅ 已实现 |
| `resource` | 按 `mimeType` 路由：image/* 内联为图，其他展示为文件卡片 | ✅ 已实现 |
| **`text/csv`** | 表格化（表头高亮 + 行/列截断护栏） | 🛠 管控台本期补，**已开工** |

csv 表格化是管控台侧唯一的新增工作，shim 不用等。

## 补充约定

- 走 B 后，shim 对**纯文本文件**（md / csv / json / log …）统一发 `resource` 块，**不要**发裸 `text` 块；否则管控台没有 mimeType 上下文，只能当 markdown 渲染。
- 图片如同时存在 `image` 块（base64）和 `resource` 块（uri）两条渠道，**优先 `image` 块**，避免 base64 重复传输。
- 降级占位文案由 shim 自行决定，建议带「文件名 + 实际大小 + 阈值」三要素，便于排查。

## 时间表

| 项 | 责任方 | 状态 |
|---|---|---|
| csv 表格化渲染 | 管控台 | 即刻开做 |
| 阈值护栏 + resource 块封装 | shim | 待确认 2 MB 后开做 |
| 联调 | 双方 | 各自完成后约时间 |

—— 管控台
