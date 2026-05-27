# analysis_params — 用户自定义分析参数

## 格式规范

前后端分离存储。API 存原始 UI-rich JSON，注入 LLM 前洗为干净 KV。

### 后端存储格式（UI-rich JSON）

```json
[
  {"key":"分析时间颗粒度","value":"月","options":["日","周","月","季","年"]},
  {"key":"语言偏白话","value":true},
  {"key":"语言风格","value":""},
  {"key":"分析深度","value":4},
  {"key":"置信阈值","value":0.85,"min":0,"max":1}
]
```

每个 item 的字段：

| 字段 | 必填 | 类型 | 说明 |
|------|------|------|------|
| `key` | 是 | string | 参数名，LLM 可见 |
| `value` | 是 | any | 参数值 |
| `options` | 否 | string[] | 有则前端渲染为下拉选择 |
| `type` | 否 | string | 显式声明控件类型：`number`/`slider`/`text`/`toggle` |
| `min`/`max` | 否 | number | 数字/滑条的取值范围，默认 0/1 |

### 控件类型推断规则（前端）

自上而下：

1. `options` 存在且为数组 → `<select>` 下拉框
2. `value` 为 `boolean` → 拨杆开关
3. `type` 为 `"number"` → 数字步进器
4. `type` 为 `"slider"` 或 `value` 为 [0,1] 浮点数 → 滑条
5. 默认 → 文字输入框

### LLM 注入格式（wash 后）

```text
分析时间颗粒度: 月
语言偏白话: true
语言风格:
分析深度: 4
置信阈值: 0.85
```

每行 `key: value`，`true`/`false` 按原样输出。插入在 `【用户分析参数】` section 中。

### API

- `GET /api/analysis-params` → 返回原始存储内容（不洗）
- `PUT /api/analysis-params` → 存入原始内容，做基础校验

### 校验规则（PUT）

1. 尝试解析为 JSON
2. 解析成功且为数组 → 逐项检查 `key` + `value` 字段存在，缺失的自动补空
3. 解析成功且为对象/字符串 → 原样存（向后兼容）
4. 解析失败 → 当字符串存（fallback）
