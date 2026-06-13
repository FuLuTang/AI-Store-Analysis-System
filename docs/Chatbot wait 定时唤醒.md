# Chatbot wait 定时唤醒

本文档说明账号级 Chatbot 的 `wait` 工具、后台调度文件和触发行为。

## 1. 能力边界

`wait` 是 chatbot 专用工具，用于登记未来唤醒任务。它不阻塞当前 Agent Loop。

适用场景：

- 用户要求倒计时、稍后提醒、指定时间提醒。
- Agent 提交异步任务、调用外部 API 后，需要稍后查询状态或继续总结。
- 小任务只需要短暂等待时，可以空参数调用，系统默认 1 秒后唤醒。

不适用场景：

- 诊断/价格推荐主流程 Agent 不暴露该工具。
- `run_python` 不再提供 `wait_seconds`；长耗时脚本由工具自身超时返回，后续如需继续查询应显式使用 `wait`。

## 2. 工具参数

| 参数名 | 类型 | 是否必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `mode` | string | 否 | `delay` | `delay` 表示多少秒后继续；`alarm` 表示指定时间提醒。 |
| `delay_seconds` | integer | 否 | `1` | `delay` 模式使用；不传或解析失败时默认 1 秒。 |
| `run_at` | string | 否 | - | `alarm` 模式使用，ISO 时间字符串，如 `2026-06-12T18:30:00+08:00`；精度按分钟处理。 |
| `resume_prompt` | string | 否 | `请根据前文和最新上下文继续处理。` | 到点后给 Agent 的恢复指令。 |
| `reason` | string | 否 | `""` | 管理员查看调度文件时使用的简短原因。 |

返回示例：

```json
{
  "ok": true,
  "scheduled": true,
  "id": "7c7509fe1af346cc85d558d38f71721c",
  "mode": "delay",
  "run_at": "2026-06-12T10:18:54+08:00"
}
```

## 3. 调度文件

待触发任务统一保存在：

```text
storage/accounts/chatbot_scheduler.jsonl
```

该文件只保存未触发任务。触发后任务会从文件中移除，不额外记录 done。

管理员取消任务：

```bash
nano storage/accounts/chatbot_scheduler.jsonl
```

删除对应 JSONL 行即可取消该闹钟。

## 4. 到点行为

`delay` 模式：

- 到点后直接唤醒对应账号的 Agent Loop。
- 不向 `chatbot/chat.jsonl` 追加消息；系统只在本次内存上下文尾部临时拼接一条纯 `system` 消息，记录设置时间、到期时间、当前时间和 `resume_prompt`。

`alarm` 模式：

- 到点后先向对应账号的 `chatbot/chat.jsonl` 追加一条消息：

```json
{
  "role": "system",
  "name": "notice",
  "content": "提醒内容",
  "datetime": "2026-06-12T18:30:00+08:00"
}
```

- 然后唤醒对应账号的 Agent Loop。

## 5. 并发与重试

- 后台 scheduler 每秒扫描一次调度文件。
- 如果对应账号 chatbot 正在运行，到期任务会保留在调度文件中，下次扫描再触发。
- 每轮同一账号只触发一个到期任务，避免同时写同一份聊天历史。
- 调度文件重写使用 `.tmp` + `replace`。如果扫描期间文件被人工修改，本轮放弃重写，下轮重新读取。
