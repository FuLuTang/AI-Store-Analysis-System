# AI分析系统 API 参考

## 1. 获取通行证 (User Key)

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/register` |
| **Header** | 无需 |
| **Body (JSON)** | `{"apiKey": "sk-..."}` 或 `{"openaiKey": "sk-..."}` (可选，设置 LLM API Key) |
| **成功返回** | `{"userKey": "fzt_...", "status": "ok"}` |
| **错误 (429)** | `{"detail": "注册过于频繁，请稍后再试"}` |

返回以 `fzt_` 开头的唯一通行证，完整 Key 仅在本次响应中出现，后续不可重新获取。

---

## 2. 校验 Key 与配置

### 校验 Key 是否可用

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/verify` |
| **Header** | `x-fzt-key: <key>`（必填） |
| **成功返回** | `{"status": "ok", "userKey": "fzt_ab...0123"}` |
| **错误 (401)** | `{"detail": "Invalid or expired key"}` |

### 查看当前配置

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/verify` |
| **Header** | `x-fzt-key: <key>`（必填） |
| **成功返回** | `{"status": "ok", "userKey": "fzt_ab...0123"}` |

`userKey` 为掩码版 Key 前段，用于身份识别。

---

## 3. 提交数据

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/analyze` |
| **Header** | `x-fzt-key: <key>`（必填） |
| **Content-Type** | `multipart/form-data` |
| **Body** | `files` 字段（一个或多个文件，每文件 ≤ **5MB**）；可选 `reasoningEffort` 字段 (`low` / `medium` / `high`，默认 `medium`) |
| **支持格式** | `.json` / `.xlsx` / `.csv` |
| **成功返回** | `{"status": "started", "pipeline": "multifile"}` |

| 状态码 | 含义 |
| :--- | :--- |
| `400` | 任务运行中 |
| `400` | 文件超过 5MB |
| `400` | JSON 解析失败 |
| `400` | 文件读取失败 |

---

## 4. 监控进度

分析耗时几十秒至数分钟，取决于数据量与模型。

### 实时状态

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/status` |
| **Header** | `x-fzt-key: <key>`（必填） |

**返回字段**:
```json
{
  "status": "running",
  "errorMessage": "",
  "result": "...",
  "fullResult": "..."
}
```

| 字段 | 含义 |
| :--- | :--- |
| `status` | `idle` 空闲 / `running` 运行中 / `completed` 完成 / `error` 失败 / `aborted` 已终止 |
| `errorMessage` | 错误详情（仅 `error` / `aborted` 时非空） |
| `result` | JSON 简化报告（仅 `completed` 时非空） |
| `fullResult` | Markdown 完整报告（仅 `completed` 时非空） |

### 实时日志流 (SSE)

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/stream?x-fzt-key=<key>`（必填，通过 Query 传 Key） |
| **Content-Type** | `text/event-stream` |

每 0.5 秒推送日志事件，首条为 `{"type":"reset","time":"HH:MM:SS"}`。

### 日志快照

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/logs` |
| **Header** | `x-fzt-key: <key>`（必填） |
| **成功返回** | `[{...}, {...}]` — 日志事件数组 |

每条日志含 `type`、`time`、`nodeId` 等字段。

---

## 5. 获取报告

`/api/status` 的 `status` 为 `completed` 时即可读取：
- `result`: JSON 简化数据（适合入库）。
- `fullResult`: Markdown 完整诊断报告（适合前端展示）。

---

## 6. 其他操作

### 紧急停止

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/stop` |
| **Header** | `x-fzt-key: <key>`（必填） |
| **成功返回** | `{"status": "ok"}` |

仅在 `status` 为 `running` 时生效。

### 获取示例数据

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/examples` |
| **成功返回** | `{"files": [{"name": "概览-日.json", "base64": "..."}]}` |

返回 Base64 编码的示例文件列表，供调试器自动拉取测试。

---

## 常见问题

1. **401 Unauthorized** — 检查 Header 名 `x-fzt-key` 及 Key 是否完整。
2. **400 Bad Request (Busy)** — 同一 Key 不可并发，等待当前任务结束或调用 `/api/stop`。
3. **示例数据** — `GET /api/examples` 获取系统内置示例数据包。

---

> 调试建议：配套 **AI 系统 API 调试器 (GUI)**，输入 Key 即可点选操作。
