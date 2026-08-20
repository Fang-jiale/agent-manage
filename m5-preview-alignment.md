# ywcoder 文件/图片预览功能 —— 需管控台/AgentClient 确认

> 面向：AgentClient / 管控台团队
> 目的：ywcoder 侧要支持「agent 读到/产出的文件与图片在网页预览」，有两点需要你们拍板/确认。

## 背景（一句话）

ywcoder-shim 会把工具结果（tool_result）里的内容转成 `stream.chunk` 消息，经 AgentClient 透传到网页。数据路径：

```
ywcoder ─► shim ──stdout JSONL──► AgentClient ──WebSocket──► 网关 ──WS──► 浏览器
                                                          └─► 落一份到历史库
```

预览功能就是让 shim 把**文本/图片/资源**内容发出去、网页渲染。以下两点需对齐。

---

## 一、传输阈值与超限护栏

大图片（base64）或大文本（几 MB 的 csv/日志/由 docx·xlsx 抽取出的文本）如果原样塞进一行 JSON 发出去，会撑爆 WebSocket 单条消息、拉高延迟、把历史库撑大，甚至可能**直接断连**。

**shim 侧计划做「大小护栏」**（对管控台透明，你们无需为此做特殊逻辑）：
- 阈值**对 `text` / `image` / `resource` 所有内容块生效**（不只图片）。
- 单块内容超过阈值时 shim **不发大数据、改发一个小占位**：
  - **图片 / 二进制** → 一条提示文字（如 `[图片 chart.png 1.8MB 过大，未内联预览]`）；
  - **文本** → 截断 + 标注（如保留前 N KB + `[已截断，原文 3.2MB]`）。
- **超限 = 优雅降级**：任务照常完成、文件仍在员工终端磁盘上，只是网页这次看不到全量预览。

**❓需你们确认：**
1. **阈值取多少**？（我方默认建议 **1MB**）
2. **网关 WebSocket 单条消息大小上限是多少**？—— shim 的阈值必须**卡在它之下**，否则超限消息会在网关层被截断/拒收。请告知这个上限值。

---

## 二、传输格式与渲染分工

shim 传给网页的是**原始 typed content 数组**，**不是**渲染好的 HTML/docx/xlsx。例如读一个 md 文件，浏览器收到的是原样 markdown 文本：

```json
{"method":"stream.chunk","params":{"type":"result","content":[{"type":"text","text":"# 标题\n- 列表..."}]}}
```

**所以需要管控台做二次渲染**，分两种情况：

### 1）图片（`image` 块）—— 信息自足，好办
块里带 `data`(base64) + `mimeType`（如 `image/png`），管控台直接 `<img>` 渲染即可。

### 2）文本文件（md / csv 等）—— 有个「类型识别」的坑
shim 发的是**裸文本**，`text` 块**不带「这是 markdown 还是 csv」的类型信息**，管控台怎么知道该按哪种渲染？两个方案，需拍板：

| 方案 | 做法 | 代价 |
|---|---|---|
| **A（管控台侧关联）** | 管控台把 `result` 与它前面那条 `action` chunk 关联（`action` 里带 `arguments.file_path`，如 `report.csv`），**按扩展名推断**渲染方式 | shim 不用改，逻辑全在管控台 |
| **B（shim 侧升级，我方推荐）** | shim 把文件类结果包成 **`resource` 块**（带 `uri=文件名` + `mimeType`），管控台**按 mimeType 渲染**，明确无歧义 | shim 多一点逻辑（识别文件读取、按扩展名推 mimeType） |

**❓需你们确认：**
3. **文本文件类型识别走 A 还是 B**？（我方倾向 **B**：渲染最可靠、也符合 `resource` 的设计初衷）
4. 确认**管控台侧会做二次渲染**：md 渲染 / csv 表格化 / 图片内联（`image`）/ 文件卡片（`resource`）。

---

## 需要回复的清单

1. 传输阈值取值（默认建议 1MB）？
2. 网关 WebSocket 单条消息大小上限？
3. 文本文件类型识别走 A 还是 B？
4. 确认管控台会对 text(md/csv)/image/resource 做二次渲染。

> 备注：本期为**纯预览**，不含文件下载、不含上行上传（上传依赖 vision 模型，另行规划）。docx/xlsx 等格式的读取属于 ywcoder/agent 侧的提取能力，抽取成文本后即走本预览管道，不影响本文两点。
