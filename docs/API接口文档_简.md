# AI 分析系统 API 参考

## 1. 注册与登录

### 创建账号

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/register` |
| **Header** | 无需 |
| **Body (JSON)** | `{"username": "store001", "password": "Password123"}` |
| **成功返回** | `{"token": "完整用户token", "status": "ok", "account": "sto***"}` |
| **错误 (429)** | `{"detail": "注册过于频繁，请稍后再试"}` |

### 登录账号

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/login` |
| **Header** | 无需 |
| **Body (JSON)** | `{"username": "store001", "password": "Password123"}` |
| **成功返回** | `{"token": "完整用户token", "status": "ok", "account": "sto***"}` |
| **错误 (401)** | `{"detail": "账号或密码错误"}` |

客户端必须保存并回传完整 `token`。`account` 只用于页面展示，不参与认证。

---

## 2. Token 校验与退出

### 校验 token

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/verify` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |
| **成功返回** | `{"status": "ok", "account": "sto***"}` |
| **错误 (401)** | `{"detail": "Invalid or expired token"}` |

### 创建客服 token

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/service-token` |
| **Body (JSON)** | `{"token": "完整用户token"}` |
| **成功返回** | `{"token": "完整客服token", "status": "ok", "account": "sto***"}` |

客服 token 可查看、上传、启动任务和读取结果；删除类和安全设置类操作会返回 `403`。

普通用户 token 软过期 2 小时、硬过期 12 小时；客服 token 软过期 5 分钟、硬过期 20 分钟。

### 退出登录

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/auth/logout` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |
| **成功返回** | `{"status": "ok"}` |

---

## 3. 提交数据

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/analyze` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |
| **Content-Type** | `multipart/form-data` |
| **Body** | `files` 字段（一个或多个文件，每文件 ≤ **100MB**）；可选 `reasoningEffort` / `costTier` 字段 |
| **成功返回** | `{"status": "started", "pipeline": "custom"}` |

---

## 4. 监控进度

### 实时状态

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/status` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |

```json
{
  "status": "running",
  "errorMessage": "",
  "result": "...",
  "fullResult": "...",
  "runId": "20260609T120000_ab12cd"
}
```

### 实时日志流

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/stream?auth-token=<完整token>` |
| **Content-Type** | `text/event-stream` |

### 日志快照

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/logs` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |
| **成功返回** | `{"logs": [{...}, {...}]}` |

---

## 5. 报告与其他操作

### 下载产物

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/reports/download?auth-token=<完整token>&run_id=<runId>` |
| **成功返回** | ZIP 文件 |

### 紧急停止

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `POST` |
| **URL** | `/api/stop` |
| **Header** | `X-Auth-Token: <完整token>`（必填） |
| **成功返回** | `{"status": "ok"}` |

### 获取示例数据

| 项目 | 内容 |
| :--- | :--- |
| **方法** | `GET` |
| **URL** | `/api/examples` |
| **成功返回** | `{"files": [{"name": "...", "base64": "..."}]}` |

---

## 常见问题

1. **401 Unauthorized**：检查 `X-Auth-Token` 或 `auth-token` 是否传入完整 token，或重新登录获取新 token。
2. **403 Forbidden**：客服 token 正在访问删除类或安全设置类接口。
3. **400 Bad Request (Busy)**：同一账号已有任务运行，等待结束或调用 `/api/stop`。

管理员后台仍使用独立的 `X-Admin-Token`，不属于用户 token 链路。
