# AI 门店分析系统 API 接口方案 (v2 - 带身份鉴权)

本方案在单用户分析逻辑基础上，引入了基于 **User Key** 的多租户隔离机制。

## 1. 核心流程图 (带鉴权)

```mermaid
sequenceDiagram
    participant Client as 外部系统/用户
    participant API as FastAPI 后端
    participant Auth as 鉴权/限流模块
    participant Worker as 背景分析线程
    
    Client->>API: POST /api/analyze (带 X-FZT-Key)
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
所有业务接口（除注册外）均须在 HTTP Header 中携带：
- **Header Name**: `X-FZT-Key`
- **Value**: 用户的唯一通行证（如 `fzt_abc123...`）

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

### 3.2 核心分析类 (均需 Header: X-FZT-Key)

#### [POST] /api/analyze
- **说明**: 上传文件并启动该账户下的 AI 分析。支持多文件同时上传。
- **Content-Type**: `application/json`
- **Body (JSON)**:
  ```json
  {
    "files": [
      {
        "name": "data1.json",
        "base64": "..." 
      }
    ]
  }
  ```
- **响应**: `200` (Started) | `401` (Unauthorized) | `400` (Busy)

#### [GET] /api/status
- **说明**: 获取该账户最近一次任务的状态与结果。

#### [GET] /api/logs
- **说明**: 获取该账户最近一次任务的日志快照。

#### [POST] /api/stop
- **说明**: 强行停止该账户下的分析任务。

### 3.3 其他接口
- **[GET] /api/health**: 基础检查（无需鉴权）。
- **[GET] /api/examples**: 获取示例数据（无需鉴权）。

---

## 4. 存储隔离规约
系统会根据 `X-FZT-Key` 的 SHA256 哈希值定位存储路径：
- **路径**: `/storage/accounts/{sha256(key)}/`
- **内容**: 包含该用户的 `profile.json` (配置), `latest_report.md` (报告), `latest_logs.json` (日志)。
