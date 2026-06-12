# Chatbot 资源引用与下载控制 — 执行方案

## 目标

让 chatbot（AI 客服）在 markdown 回答中引用图片和文件下载链接，同时支持对 `service_docs` 中的**部分文件做下载限制**（AI 能读、能搜索、能发现，但用户无法直接下载）。

---

## 整体链路

```
LLM 生成 markdown（含 {{AUTH_TOKEN}} 占位符）
       ↓
落盘 chat.jsonl（无真实 token）
       ↓
GET /api/chatbot/history 返回
       ↓
前端 chat.js 替换 {{AUTH_TOKEN}} → 真实 token
       ↓
marked.parse() 渲染成 HTML
       ↓
浏览器加载 <img src="...?token=xxx"> 或点击 <a href="...?token=xxx">
       ↓
后端 GET /api/chatbot/resource/{domain}/{path}?token=xxx
       ↓
校验 token → 校验 .no_download.yaml → FileResponse
```

---

## 执行步骤

### 步骤 1：新建 `download_guard.py` 模块

**文件**：`apps/api/src/download_guard.py`（新建）

**职责**：读取、缓存、查询 `storage/service_docs/.no_download.yaml`

```python
# 核心函数

load_restricted_patterns() -> list[str]
    # 读取 .no_download.yaml，返回 restricted 列表
    # 首次加载后缓存，检测文件 mtime 变化时刷新

is_downloadable(relative_path: str) -> bool
    # 判断 service_docs 下的相对路径是否允许用户下载
    # 将路径与 restricted 列表中的 glob 模式逐一匹配
    # 命中任一 restricted 模式 → 返回 False
    # 注意：匹配的是相对于 service_docs 根目录的路径

add_restricted_pattern(relative_path: str) -> None
    # 将路径添加到 .no_download.yaml 的 restricted 列表
    # 去重，写回文件，刷新缓存

remove_restricted_pattern(relative_path: str) -> None
    # 从 .no_download.yaml 的 restricted 列表中移除匹配项
    # 写回文件，刷新缓存

get_restricted_status(relative_path: str) -> bool
    # 返回该路径当前是否在 restricted 列表中
    # 供 admin 列表接口使用
```

**配置文件**：`storage/service_docs/.no_download.yaml`

```yaml
# 禁止用户下载的文件/目录（AI 仍可读取）
# 使用 glob 模式，相对于 service_docs 根目录
restricted: []
```

**glob 匹配规则**：

- `internal/**` — 匹配 `internal/` 下所有文件和子目录
- `drafts/*.md` — 匹配 `drafts/` 下的所有 .md 文件
- `secret_config.json` — 匹配根目录下的单个文件
- 使用 Python `fnmatch` 对路径的每个组件做递归匹配

---

### 步骤 2：新增资源访问接口

**位置**：`apps/api/src/main.py` 或 `apps/api/src/chatbot_service.py`

**路由**：`GET /api/chatbot/resource/{domain}/{path:path}`

**参数**：
| 参数 | 位置 | 说明 |
|---|---|---|
| `domain` | path | `chatbot` 或 `service_docs` |
| `path` | path | 域内相对路径，如 `faq/logo.png` |
| `token` | query | 登录 token |

**逻辑流程**：

```
1. 从 query 参数获取 token
2. 调用 resolve_session(token, task_type="chatbot") 校验登录
3. 校验 domain ∈ {"chatbot", "service_docs"}
4. 根据 domain 解析文件系统根目录：
   - chatbot  → {account_dir}/chatbot/workspace/
   - service_docs → storage/service_docs/
5. 拼接路径，防路径穿越（resolve + is_relative_to）
6. 检查文件是否存在且为普通文件
7. 如果是 service_docs 域：
   调用 is_downloadable(path)，不通过则返回 404
8. 返回 FileResponse（FastAPI 自动处理 Content-Type）
```

**安全要点**：

- 路径穿越防护：`full_path.resolve().is_relative_to(root.resolve())`
- 文件不存在和不可下载统一返回 404（不暴露文件是否存在）
- chatbot workspace 不做下载限制（本来就是账号私有的）
- service_docs 做 `.no_download.yaml` 检查

---

### 步骤 3：改造 Admin API

**涉及路由**（均在 `apps/api/src/main.py` 中，约 997-1117 行）：

#### 3a. `GET /api/admin/service-docs` — 列表接口

**改动**：返回的每个文件条目增加 `undownloadable` 字段

```python
# 在遍历目录条目时，对每个条目调用：
entry["undownloadable"] = get_restricted_status(relative_path)
```

这样前端可以渲染 🔒 图标。

#### 3b. `POST /api/admin/service-docs/file` — 创建/更新文件

**改动**：请求体增加可选字段 `undownloadable: bool = False`

```python
# 处理完文件写入后：
if undownloadable:
    add_restricted_pattern(relative_path)
else:
    remove_restricted_pattern(relative_path)  # 如果之前被标记过
```

#### 3c. `POST /api/admin/service-docs/upload` — 上传文件

**改动**：表单字段增加 `undownloadable`（默认 `"false"`）

逻辑同上。

#### 3d. `DELETE /api/admin/service-docs/file` — 删除文件

**改动**：删除文件时同步清理 `.no_download.yaml`

```python
# 删除文件后：
remove_restricted_pattern(relative_path)
```

---

### 步骤 4：更新 Agent 系统提示词

**文件**：`packages/agents/chatbot/assistant.md`

**新增内容**（追加到文件末尾）：

```markdown
## 引用文件与图片

当回答中需要展示 chatbot workspace 或 service_docs 中的图片、或提供文件下载时，
使用以下 URL 格式（不要使用相对路径或 file:// 协议）：

- 图片：`![描述](/api/chatbot/resource/{domain}/{path}?token={{AUTH_TOKEN}})`
- 文件下载：`[文件名](/api/chatbot/resource/{domain}/{path}?token={{AUTH_TOKEN}})`

参数说明：

- `{domain}` — `chatbot` 或 `service_docs`（即文件所属的域）
- `{path}` — 文件在域内的相对路径
- `{{AUTH_TOKEN}}` — **保持原样，不要替换**。客户端会自动填入当前用户的登录令牌

约束：

- 只能引用你通过 read_file / list_files / search 工具实际确认存在的文件
- 在生成 service_docs 文件的下载链接前，先读取 `.no_download.yaml` 文件
  确认该路径没有被 restricted 规则命中；若被命中，则只引用内容、不生成下载链接
- 不要编造不存在的文件路径
```

---

### 步骤 5：改造前端 `chat.js`

**文件**：`apps/web/public/chat.js`

#### 5a. 消息渲染前替换占位符

**位置**：`renderChatMessages()` 函数（约第 346 行）

在 `formatChatContent(msg.content)` 调用**之前**，增加一行：

```javascript
// 替换 token 占位符为当前登录用户的真实 token
const token =
  (typeof window.getAuthToken === "function"
    ? window.getAuthToken()
    : sessionStorage.getItem("authToken") || "") || "";
msg.content = msg.content.replace(/\{\{AUTH_TOKEN\}\}/g, token);
```

#### 5b. 流式消息也同样处理

**位置**：流式接收 LLM 回复并实时渲染的逻辑（如有）

同样在渲染前替换 `{{AUTH_TOKEN}}`。

> **注意**：流式场景下，token 可能被分片传输（如 `{{AUTH` 和 `_TOKEN}}` 分两次到达）。
> 简单处理：在流式内容拼接完成后渲染前做替换（即渲染时整段替换，不逐 chunk 替换）。
> 如果当前已对每个 chunk 做实时渲染，改为：渲染时对完整内容做替换，或使用缓冲策略。

#### 5c. marked 图片渲染器（可选加强）

如果希望进一步控制图片加载行为（如 loading="lazy"、错误处理），可在 marked 的自定义 renderer 中增加 image 处理：

```javascript
// 在 chat.js 的 marked.Renderer 中增加（约第 156 行附近）
renderer.image = function (href, title, text) {
  return `<img src="${href}" alt="${text}" title="${title || ""}" 
                 loading="lazy" onerror="this.style.display='none'" />`;
};
```

这项是可选的，不影响核心功能。

---

### 步骤 6：改造 Admin 前端页面

**文件**：`apps/web/public/admin.html`

#### 6a. 文件上传/编辑弹窗增加"禁止下载"开关

在文件管理浮窗的上传/编辑表单中，增加一个 checkbox：

```html
<label class="checkbox-label">
  <input type="checkbox" id="fileUndownloadable" />
  <span>禁止用户下载（AI 客服仍可读取此文件）</span>
</label>
```

#### 6b. 提交时携带 `undownloadable` 字段

在调用 `POST /api/admin/service-docs/file` 或 `/upload` 时：

```javascript
const body = {
  // ... 现有字段
  undownloadable: document.getElementById("fileUndownloadable").checked,
};
```

#### 6c. 文件列表中显示限制状态

在文件列表渲染时，对 `undownloadable: true` 的文件显示 🔒 图标和提示：

```javascript
if (entry.undownloadable) {
  html += '<span class="lock-icon" title="禁止用户下载">🔒</span>';
}
```

编辑已有文件时，根据 `undownloadable` 状态预勾选 checkbox。

---

## 执行顺序

| 步骤                   | 依赖   | 预计改动量                 |
| ---------------------- | ------ | -------------------------- |
| 1. `download_guard.py` | 无     | ~60 行（新文件）           |
| 2. 资源访问接口        | 步骤 1 | ~40 行                     |
| 3. Admin API 改造      | 步骤 1 | ~30 行（4 个路由各加几行） |
| 4. 系统提示词          | 步骤 2 | ~20 行追加                 |
| 5. 前端 chat.js        | 步骤 2 | ~10 行                     |
| 6. 前端 admin.html     | 步骤 3 | ~40 行                     |

建议按顺序执行，也可以 1→2+3 并行、4+5+6 并行。

---

## 待确认项 (用户已回答)

1. **chatbot workspace 是否需要下载限制？** 当前方案仅对 `service_docs` 做 `.no_download.yaml` 检查。chatbot workspace 是账号私有的，token 本身就做了隔离。如果需要，可以扩展。
   > 可以，做起来也用一样的，通用的，简单的方法就好。
2. **`.no_download.yaml` 的 glob 匹配粒度**：当前方案对整个路径做 `fnmatch`。是否需要支持更复杂的规则（如：仅限制下载、但允许预览）？当前按 YAGNI 原则保持简单。
   > 不需要
3. **流式消息的 token 替换**：需要确认当前 chat.js 流式渲染的具体实现，确保 `{{AUTH_TOKEN}}` 不会被分片截断。详情见步骤 5b 的注意。
   > 按你推荐的方法走，因为其实最终给用户的时候不是流式渲染，甚至连写入 chat.jsonl 都不是流式。
