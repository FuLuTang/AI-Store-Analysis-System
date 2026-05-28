# analysis_params 规范

## 1. 定位

`analysis_params` 用于传递本次任务的可变分析参数。

它的用途是：

- 约束报告表达方式
- 指定分析深度和输出侧重点
- 传入用户临时要求

当前系统里：

- API 负责存取原始参数
- Agent Loop 在构造 prompt 时注入洗后的参数文本
- `plan.json` 第 3 步也会把参数要求拼进报告生成阶段

---

## 2. 存储格式

后端存储的是 UI-rich JSON，便于前端渲染。

示例：

```json
[
  {"key":"分析时间颗粒度","value":"月","options":["日","周","月","季","年"]},
  {"key":"语言偏白话","value":true},
  {"key":"语言风格","value":""},
  {"key":"分析深度","value":4},
  {"key":"置信阈值","value":0.85,"min":0,"max":1}
]
```

字段约束：

| 字段 | 必填 | 类型 | 说明 |
|------|------|------|------|
| `key` | 是 | string | 参数名，LLM 可见 |
| `value` | 是 | any | 参数值 |
| `options` | 否 | string[] | 有则前端可渲染为下拉 |
| `type` | 否 | string | `number` / `slider` / `text` / `toggle` |
| `min` | 否 | number | 数字型下界 |
| `max` | 否 | number | 数字型上界 |

---

## 3. 前端控件推断规则

推荐顺序：

1. `options` 为数组：下拉框
2. `value` 为 `boolean`：开关
3. `type == "number"`：数字输入
4. `type == "slider"`，或 `value` 是 `[0,1]` 浮点数：滑条
5. 其他情况：文本输入框

---

## 4. 注入给 LLM 的格式

注入前需要把 UI-rich JSON 清洗为纯文本 KV。

示例：

```text
分析时间颗粒度: 月
语言偏白话: true
语言风格:
分析深度: 4
置信阈值: 0.85
```

要求：

- 每行 `key: value`
- `true/false` 按原样输出
- 空值可保留空字符串
- 不把 `options/min/max/type` 等 UI 元数据喂给模型

---

## 5. 注入位置

当前 custom workflow 中，`analysis_params` 会影响两处：

### 5.1 Prompt

`AgentLoop` 初始化消息时：

- `system`: `build_system_content(self.analysis_params)`
- `user`: `build_user_content(self.ws, self.analysis_params)`

这里决定模型一开始如何理解用户的分析偏好。

### 5.2 Plan 第 3 步

`CustomPipeline._write_plan()` 会把参数文本注入报告写作步骤的 detail 中：

- 用于提醒模型在写 `summary.md` 时优先遵循这些用户要求

执行影响：

- `analysis_params` 影响报告表达风格
- `analysis_params` 影响报告结构和详略

---

## 6. API

当前接口：

- `GET /api/analysis-params`
- `PUT /api/analysis-params`
- `GET /api/analysis-params/presets`

职责边界：

- API 不做深层业务解释
- API 只做基础格式校验和存储
- 真正如何使用这些参数，由 prompt 和 workflow 决定

---

## 7. PUT 校验规则

1. 尝试解析为 JSON
2. 如果是数组，则逐项检查 `key` 和 `value`
3. 缺字段时自动补默认值
4. 如果是对象或字符串，允许原样存，兼容历史数据
5. 解析失败时，退回字符串存储

该策略用于兼容历史存量数据。

---

## 8. 设计原则

### 8.1 应该放进 `analysis_params` 的内容

- 语言风格
- 分析深度
- 时间粒度偏好
- 是否更偏结论导向
- 是否更偏行动建议
- 某次分析的临时关注点

### 8.2 不应该放进 `analysis_params` 的内容

- 指标定义
- 字段映射规则
- 工具说明
- SQL 模板
- 长期稳定的行业知识

这些内容归属于 `指标计算文档.md` 或代码逻辑，不应放入用户参数。

---

## 9. 示例

### 9.1 偏管理层表达

```json
[
  {"key":"语言偏白话","value":true},
  {"key":"分析深度","value":2},
  {"key":"输出偏好","value":"更结论导向"}
]
```

### 9.2 偏分析师表达

```json
[
  {"key":"语言偏白话","value":false},
  {"key":"分析深度","value":5},
  {"key":"输出偏好","value":"保留更多数据证据和过程说明"}
]
```

---

## 10. 当前边界

`analysis_params` 仅用于参数配置，不承载业务规则。

可影响范围：

- 怎么说
- 说多深
- 优先讲什么

不可替代范围：

- 文件解析策略
- 指标口径定义
- 工具能力边界
- workspace 执行流程
