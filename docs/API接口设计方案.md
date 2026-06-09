# AI 门店分析系统 API 接口方案 (v3.0 - 账号密码与 Token 鉴权)

本方案在单用户分析逻辑基础上，引入账号名、密码登录与临时 token 鉴权机制。客户端登录后保存完整 token，后续用户侧接口通过 `X-Auth-Token` Header 或 `auth-token` 查询参数携带 token。

## 1. 核心流程图 (带鉴权)

```mermaid
sequenceDiagram
    participant Client as 外部系统/用户
    participant API as FastAPI 后端
    participant Auth as 鉴权/限流模块
    participant Worker as 背景分析线程

    Client->>API: POST /api/auth/login
    API->>Auth: 校验账号名与 bcrypt 密码哈希
    API-->>Client: 返回完整 token
    Client->>API: POST /api/analyze (multipart + X-Auth-Token)
    API->>Auth: 校验 token 日志与软/硬过期
    alt token 错误或过期
        API-->>Client: 401 Unauthorized
    else 系统忙碌 — 该用户任务进行中
        API-->>Client: 400 Bad Request
    else 校验通过且空闲
        API->>API: 加载用户专属配置与旧日志
        API->>Worker: 启动异步计算
        API-->>Client: 200 OK — Started
    end

    Worker->>Worker: 分析中...
    Worker->>API: 写入 storage/accounts/{account_id}/runs/diagnosis/{run_id}/workspace/output
```

## 2. 鉴权规范

### 2.1 用户鉴权

用户接口通过 HTTP Header 或 SSE 查询参数携带完整 token：

- **Header Name**: `X-Auth-Token`
- **Query Name**: `auth-token`，用于 `EventSource` 或下载链接等不方便设置 Header 的场景
- **Value**: 登录或注册接口返回的完整 token

> 所有核心用户接口均要求提供有效 token，不传 token 或 token 过期将返回 401 Unauthorized。接口返回的 `account` 仅为 `abc***` 形式的展示字段，不参与鉴权或账号定位。

### 2.2 管理员鉴权

管理员接口通过 HTTP Header 携带：

- **Header Name**: `X-Admin-Token`
- **Value**: 与服务端环境变量 `ADMIN_TOKEN` 一致的口令
- **说明**: 管理员鉴权独立于用户 token / 客服 token 链路。

---

## 3. API 接口参考手册

### 3.1 账号管理类

#### [POST] /api/auth/register

- **说明**: 创建账号，写入 bcrypt 密码哈希，并返回登录 token。受限流保护（3分钟内5次）。
- **Body (JSON)**: `{"username": "store001", "password": "Password123"}`
- **响应 (200)**: `{"token": "完整用户token", "status": "ok", "account": "sto***"}`
- **错误 (429)**: 注册过于频繁
- **错误 (500)**: 账号初始化失败

#### [POST] /api/auth/login

- **说明**: 使用账号名和密码登录。
- **Body (JSON)**: `{"username": "store001", "password": "Password123"}`
- **响应 (200)**: `{"token": "完整用户token", "status": "ok", "account": "sto***"}`
- **错误 (401)**: 账号或密码错误

#### [POST] /api/auth/verify

- **说明**: 校验当前 token 是否有效。
- **Header**: `X-Auth-Token` (必填)
- **响应 (200)**: `{"status": "ok", "account": "sto***"}`
- **错误 (401)**: Invalid or expired token

#### [POST] /api/auth/service-token

- **说明**: 使用有效用户 token 创建客服 token。
- **Body (JSON)**: `{"token": "完整用户token"}`
- **响应 (200)**: `{"token": "完整客服token", "status": "ok", "account": "sto***"}`
- **错误 (401)**: 用户 token 无效或已过期

#### [POST] /api/auth/logout

- **说明**: 撤销当前 token。
- **Header**: `X-Auth-Token` (必填)
- **响应 (200)**: `{"status": "ok"}`

### 3.2 核心分析类

#### [POST] /api/analyze

- **说明**: Multipart 上传入口，提交文件并启动分析。
- **Header**: `X-Auth-Token`（必填）
- **Body (Multipart)**: `files` 字段，一个或多个文件。每文件最大 **100MB**，支持 `.json` / `.xlsx` / `.csv` 格式。可选字段 `reasoningEffort` (`low` / `medium` / `high`，默认 `medium`)。
- **响应 (200)**: `{"status": "started", "pipeline": "multifile"}`
- **错误 (400)**: 任务正在运行中 / 文件过大(>100MB) / JSON 解析失败 / 文件读取失败

#### [GET] /api/status

- **说明**: 获取该账户最近一次任务的状态与结果。
- **Header**: `X-Auth-Token`（必填）
- **响应 (200)**:

```json
{
  "status": "idle"|"running"|"completed"|"error"|"aborted",
  "errorMessage": "",
  "result": "JSON 格式简化报告 (completed 时有值)",
  "fullResult": "Markdown 格式完整报告 (completed 时有值)"
}
```

#### [GET] /api/logs

- **说明**: 获取该账户最近一次任务的日志快照（全量日志数组）。
- **Header**: `X-Auth-Token`（必填）
- **响应 (200)**: `[{log_entry}, ...]` — 日志事件数组，每条含 `type`、`time`、`nodeId` 等字段。

#### [GET] /api/stream

- **说明**: SSE 日志流，供前端实时刷新监控面板。
- **Query**: `?auth-token=...`（必填，通过 URL 查询参数传递，适配 EventSource 场景）
- **响应**: `text/event-stream`，首条事件 `{"type":"reset","time":"HH:MM:SS"}`，后续为日志事件 JSON

#### [POST] /api/stop

- **说明**: 强行停止该账户下的分析任务。
- **Header**: `X-Auth-Token`（必填）
- **响应 (200)**: `{"status": "ok"}`

### 3.3 客服会话类

#### [GET] /api/chatbot/history

- **说明**: 读取当前账号的客服会话历史消息。
- **Header**: `X-Auth-Token`（必填）
- **响应 (200)**:

```json
{
  "messages": [
    {
      "role": "notice",
      "content": "客服会话已接入",
      "datetime": "2026-06-05T10:29:58+08:00"
    },
    {
      "role": "user",
      "content": "...",
      "datetime": "2026-06-05T10:30:00+08:00"
    },
    {
      "role": "assistant",
      "content": "...",
      "datetime": "2026-06-05T10:30:05+08:00"
    }
  ]
}
```

- **说明补充**: 历史数据存储在 `storage/accounts/{account}/chatbot/chat.jsonl`，每行一条消息记录。`user` 和 `assistant` 消息会额外带 `datetime` 字段；用户消息如果引用附件，会带 `attachments` 元数据。
- **Notice 规则**:
  - `chat.jsonl` 中 `{"role":"system","name":"notice","content":"...","time":"..."}` 会在接口响应中转换为 `{"role":"notice","content":"...","datetime":"..."}`。
  - 普通 `role=system` 消息不会通过历史接口返回给前端。

#### [POST] /api/chatbot/attachments

- **说明**: 上传客服会话附件。图片、PDF、Excel、CSV、压缩包等都按附件处理。
- **Header**: `X-Auth-Token`（必填）
- **Body**: `multipart/form-data`
  - `attachments`: 附件文件，可传多个
- **限制**:
  - 单个附件最大 100MB。
  - 单次最多上传 20 个附件。
- **响应 (200)**:

```json
{
  "attachments": [
    {
      "attachmentId": "f4a1...",
      "originalName": "门店照片.png",
      "storedName": "f4a1....png",
      "mimeType": "image/png",
      "size": 123456,
      "sha256": "...",
      "createdAt": "2026-06-05T10:30:00+08:00",
      "relativePath": "chatbot/files/f4a1....png"
    }
  ]
}
```

- **同名规则**:
  - `originalName` 只用于展示、日志和下载名，不参与服务器唯一性判断。
  - 服务器真实文件名使用 `attachmentId + 原扩展名`，因此同名附件不会覆盖。
  - 同名不同内容允许上传，会得到不同 `attachmentId`。
  - 同名同内容暂时也按两次上传处理，后续可按 `sha256` 做去重优化。
- **存储位置**:
  - 附件元数据：`storage/accounts/{account}/chatbot/attachments.jsonl`
  - 附件文件：`storage/accounts/{account}/chatbot/files/{attachmentId}.{ext}`

#### [POST] /api/chatbot

- **说明**: 发送一条聊天消息，和账号下的客服会话交互，并以流式文本返回结果。
- **Header**: `X-Auth-Token`（必填）
- **Body (JSON)**:

```json
{
  "content": "用户输入内容",
  "attachmentIds": ["f4a1..."]
}
```

- **接收字段**:
  - `content`: 主字段；如果本次只上传附件，可以不传或传空字符串
  - `attachmentIds`: 本次会话引用的附件 ID 列表，来自 `/api/chatbot/attachments`
  - `message`: 旧字段，保留但不再作为主要口径
  - `text`: 旧字段，保留但不再作为主要口径

- **说明补充**: 这是对外的简化消息接口，客户端只提交本次输入和附件引用。
- **响应**: `text/plain; charset=utf-8` 的流式文本，不是 JSON。
- **行为说明**:
  - 服务端会读取当前账号的客服历史消息，并将 `assistant.md` 作为动态系统提示词放到本次模型上下文最前面。
  - `assistant.md` 只在发送给模型前动态拼接，不写入 `chat.jsonl`。
  - 服务端会将客服历史消息、本次输入和本次附件清单一起作为会话上下文；历史中的普通 `system` 消息和 `system/name=notice` 消息不发送给模型。
  - 如果只有附件没有文字输入，服务端会按“请查看本次上传的附件。”处理本次输入。
  - 本次附件会以元数据和相对路径形式进入会话上下文；客服会话处理逻辑必要时可读取 `chatbot/files/` 下的附件内容。
  - 读类工具统一使用域名前缀路径，当前只支持 `chatbot/...` 与 `service_docs/...` 两个域；`service_docs` 用于公司公共资料，`chatbot` 用于私有会话工作区。
  - `list_files`、`read_file`、`read_document_structure`、`search` 这类读工具返回的 `path` 也会保留域名前缀，方便模型继续引用同一路径。

- **相关配置**: 客服会话的连接参数来自全局预设 `chatbot` 段，由管理员接口维护，并通过 `/api/admin/llm-presets` 读取和更新。

### 3.4 管理员接口 (需 Header: X-Admin-Token)

#### [GET] /api/admin/llm-presets

- **说明**: 读取 low/medium/high 三档全局 LLM 预设（包含独立的 `call` 与 `fastcall` 配置，并且在根级平铺挂载 `call` 的参数以向下兼容旧有调用）。
- **补充**: 同一份预设中还包含 `chatbot` 独立连接配置，供 `/api/chatbot` 使用。
- **响应 (200)**: `{"status": "ok", "presets": {"low": {"call": {"baseUrl": "...", "apiKey": "...", "model": "...", "reasoningEffort": "..."}, "fastcall": {...}, "baseUrl": "...", "apiKey": "...", "model": "..."}, "medium": {...}, "high": {...}}}`

#### [POST] /api/admin/llm-presets

- **说明**: 更新全局 LLM 预设。
- **Body (JSON)**:
  - 推荐方式 A: `{"presets": {"low": {"call": {...}, "fastcall": {...}}, "medium": {...}, "high": {...}}}`
  - 兼容方式 B: `{"low": {"call": {...}, "fastcall": {...}}, "medium": {...}, "high": {...}}`
  - 旧版平铺兼容方式: 若单层直接传入 `baseUrl`, `apiKey`, `model` 等字段，后端会自动转换并同时应用于 `call` 与 `fastcall`。
  - 子对象字段（`call` 与 `fastcall`）：`baseUrl` (string), `apiKey` (string, 可选), `apiKeyEnc` (string, 可选), `model` (string), `reasoningEffort` (string)
- **补充**: `chatbot` 配置字段为 `baseUrl`、`apiKey`、`model`，由独立聊天接口使用，不参与 `low/medium/high` 三档分析预设。
- **响应 (200)**: `{"status": "ok", "presets": {"low": {...}, "medium": {...}, "high": {...}}}`

#### [GET] /api/admin/service-docs

- **说明**: 列出 `storage/service_docs/` 下的文件与目录。
- **Query**: `path`，默认 `service_docs/`
- **响应 (200)**:

```json
{
  "status": "ok",
  "path": "service_docs/",
  "entries": [
    {
      "path": "service_docs/policy/faq.md",
      "name": "faq.md",
      "isDir": false,
      "kind": "markdown",
      "size": 1234,
      "sizeHuman": "1234B",
      "ext": ".md"
    }
  ]
}
```

#### [GET] /api/admin/service-docs/file

- **说明**: 读取 `service_docs` 中的文本文件内容。
- **Query**: `path`，必须带域名前缀，例如 `service_docs/policy/faq.md`
- **响应 (200)**: `{"status":"ok","path":"service_docs/...","content":"..."}`

#### [POST] /api/admin/service-docs/file

- **说明**: 写入或覆盖 `service_docs` 中的文本文件。
- **Body (JSON)**:
  - `path`: 带域名前缀的路径，例如 `service_docs/policy/faq.md`
  - `content`: 文本内容
- **响应 (200)**: `{"status":"ok","path":"service_docs/...","bytesWritten":123}`

#### [POST] /api/admin/service-docs/upload

- **说明**: 上传一个二进制或文本文件到 `service_docs`。
- **Body**: `multipart/form-data`
  - `file`: 文件
  - `path`: 可选，带域名前缀的目标路径；不传时默认使用文件名
- **响应 (200)**: `{"status":"ok","path":"service_docs/...","bytesWritten":123,"mimeType":"..." }`

#### [DELETE] /api/admin/service-docs/file

- **说明**: 删除 `service_docs` 中的文件或目录。
- **Query**: `path`，必须带域名前缀
- **响应 (200)**: `{"status":"ok","path":"service_docs/..."}`

### 3.5 其他接口

- **[GET] /api/examples**: 获取示例数据，返回 `{"files": [{"name": "...", "base64": "..."}]}`。无需鉴权。

---

## 4. 存储隔离规约

系统会根据账号名生成账号目录，token 只用于请求鉴权：

- **路径**: `/storage/accounts/{账号名前三位}_{SHA256(账号名)前六位}/`
- **内容**: 包含该账号的 `account.json`、`account_tokens.jsonl`、`profile.json`、`analysis_params.json`、`runs/` 等数据。
- **任务目录**: `/storage/accounts/{account_id}/runs/{task_type}/{run_id}/`
