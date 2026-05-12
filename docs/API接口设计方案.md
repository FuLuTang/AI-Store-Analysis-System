# AI 门店分析系统 API 接口方案 (v2.1 - 带身份鉴权)

本方案在单用户分析逻辑基础上，引入了基于 **User Key** 的多租户隔离机制。

## 1. 核心流程图 (带鉴权)

```mermaid
sequenceDiagram
    participant Client as 外部系统/用户
    participant API as FastAPI 后端
    participant Auth as 鉴权/限流模块
    participant Worker as 背景分析线程
    
    Client->>API: POST /api/run (JSON 格式，兼容前端工作台)
    API->>Auth: 校验 Key 有效性
    alt Key 错误
        API-->>Client: 401 Unauthorized
    else 系统忙碌 (该用户任务进行中)
        API-->>Client: 400 Bad Request
    else 校验通过且空闲
        API->>API: 加载用户专属配置与旧日志
        API->>Worker: 启动异步计算
        API-->>Client: 200 OK (Started)
    fi

    Worker->>Worker: 分析...
    Worker->>API: 写入 storage/accounts/{hash}/latest_report.md
```

## 2. 鉴权规范

### 2.1 用户鉴权
用户接口通过 HTTP Header 携带：
- **Header Name**: `X-FZT-Key`
- **Value**: 用户的唯一通行证（如 `fzt_abc123...`）

> legacy 会话说明：当接口标注“`X-FZT-Key` 可选”且请求未携带该 Header 时，后端会回落到系统内置的 legacy 账号上下文，用于兼容旧版前端流程。

### 2.2 管理员鉴权
管理员接口通过 HTTP Header 携带：
- **Header Name**: `X-Admin-Token`
- **Value**: 与服务端环境变量 `ADMIN_TOKEN` 一致的口令

---

## 3. API 接口参考手册

### 3.1 账号管理类

#### [POST] /api/auth/register
- **说明**: 申请一个新的 User Key。受限流保护（3分钟内5次）。
- **Body (JSON)**: `{"openaiKey": "可选的 AI Key"}`
- **响应**: `{"userKey": "fzt_完整Key_仅显示一次", "status": "ok"}`

#### [GET] /api/auth/me
- **说明**: 检查当前 Key 的有效性及关联配置。
- **Header**: `X-FZT-Key`
- **响应**: `{"userKey": "fzt_掩码版", "config": {...}}`

#### [POST] /api/auth/verify
- **说明**: 快速校验当前 Key 是否可用。
- **Header**: `X-FZT-Key`
- **响应**: `{"status":"ok","userKey":"fzt_掩码版"}`

### 3.2 配置类

#### [GET] /api/config
- **说明**: 读取当前会话配置（如 `reasoningEffort`、模型、是否已配置 API Key）。
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）

#### [POST] /api/config
- **说明**: 更新当前会话配置。
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）

### 3.3 核心分析类

#### [POST] /api/run
- **说明**: 前端主流程入口，提交 JSON 文件数组并启动分析。
- **Body (JSON)**:
  - `files` (必填, array): 解析后的业务 JSON 数据列表
  - `filenames` (可选, array<string>): 与 `files` 对应的原始文件名
  - `settings` (可选, object): 本次任务覆盖配置（如 `reasoningEffort`）
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）
- **响应**: `200` (Started) | `400` (Busy)

#### [POST] /api/analyze
- **说明**: Multipart 上传入口，兼容上传式调用。
- **Body (Multipart)**: `files: [...]`
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）
- **响应**: `200` (Started) | `400` (Busy)

#### [GET] /api/status
- **说明**: 获取该账户最近一次任务的状态与结果。
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）

#### [GET] /api/logs
- **说明**: 获取该账户最近一次任务的日志快照。
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）

#### [GET] /api/stream
- **说明**: SSE 日志流，供前端实时刷新监控面板。
- **Query**: `x-fzt-key`（可选）

#### [POST] /api/stop
- **说明**: 强行停止该账户下的分析任务。
- **Header**: `X-FZT-Key`（可选，不传时走 legacy 会话）

### 3.4 管理员接口 (需 Header: X-Admin-Token)

#### [GET] /api/admin/llm-presets
- **说明**: 读取 low/medium/high 三档全局 LLM 预设。

#### [POST] /api/admin/llm-presets
- **说明**: 更新全局 LLM 预设。
- **Body (JSON)**:
  - 推荐方式 A: `{"presets": {"low": {...}, "medium": {...}, "high": {...}}}`（结构稳定，便于后续扩展额外字段）
  - 兼容方式 B: `{"low": {...}, "medium": {...}, "high": {...}}`（仅用于兼容历史调用方）
  - 单档对象字段：`baseUrl` (string), `apiKey` (string, 可选), `model` (string)

### 3.5 其他接口
- **[GET] /api/health**: 基础检查（无需鉴权）。
- **[GET] /api/examples**: 获取示例数据（无需鉴权）。

> 兼容性说明：当前前端工作台默认通过 `POST /api/run` 启动分析，后端需持续保留该端点兼容。

---

## 4. 存储隔离规约
系统会根据 `X-FZT-Key` 的 SHA256 哈希值定位存储路径：
- **路径**: `/storage/accounts/{sha256(key)}/`
- **内容**: 包含该用户的 `profile.json` (配置), `latest_report.md` (报告), `latest_logs.json` (日志)。
