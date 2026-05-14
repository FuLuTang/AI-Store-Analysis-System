# Agent 方案 — 待定

> 本文档定位：记录我们打算引入 Agent 来做什么，以及设想的流程。**具体用哪个 Agent 框架 / SDK 尚待讨论，欢迎各位大佬给建议。**

---

## 动机

目前管线确定性部分的代码（`input_adapter` → `semantic_mapper` → `metric_engine`）存在两个硬伤：

1. **输入格式脆弱**：深层嵌套 JSON、不规则结构直接歇菜
2. **字段映射硬编码**：`KEYWORD_MAP` 死字典，新行业/新字段需改代码

想做的：用 Agent + 工具调用（Python 执行 + SQL 执行），让 LLM 动态处理"看不懂"的数据。

---

## 想让 Agent 干的事

```
数据输入（任意格式）
  ↓ Agent: Python Tool — 写代码解析 & 展平为二维表
  ↓ Agent: 写入 DuckDB — 结构化存查
  ↓ Agent: 读上下文文档 — 匹配标准字段名
  ↓ Agent: SQL Tool — 写 SQL 算指标
  ↓ [确定性层: Threshold → Evidence → LLM Report Writer]
```

具体每步的输入输出见 `docs/架构设计.md` 中核心设计 1~4 节。

---

## 技术边界

- **后端语言**：Python（现有 FastAPI 项目）
- **LLM 端点**：已有 OpenAI-compatible API（deepseek），支持 function calling
- **SQL 引擎**：倾向 DuckDB（零依赖，单进程，原生支持 pandas/parquet/JSON）
- **任务类型**：单 Agent 序列执行（不是多 Agent 协作），每次调用完成一步

---

## 两个对照分支

大佬决策：做两个分支对照验证，**Pydantic AI 为主线候选，smolagents 为对照 PoC**。

| 分支 | 定位 | 深度 | 评判标准 |
|------|------|------|---------|
| `pydantic-ai-agent` | 主线候选，生产级 | AgentService + tools + structured output + FastAPI route | 稳定展平、SQL 可靠、DeepSeek 兼容、工程舒服 |
| `smolagents-codeagent-poc` | 对照实验，快速验证 | CodeAgent 写 Python 展平 → DuckDB 查询 → 输出 | 脏数据鲁棒性、代码生成稳定性 |

策略：

- 默认押 **Pydantic AI**
- 如果 smolagents 处理脏数据明显更强 → 将其 Python 代码生成能力抽取为解析工具子模块，主框架仍是 Pydantic AI
- 如果 smolagents 只是 demo 骚但生产接入麻烦 → 弃

详细设计：[/docs/agent-poc/pydantic-ai-agent/文档.md](./agent-poc/pydantic-ai-agent/文档.md) 和 [/docs/agent-poc/smolagents-codeagent-poc/文档.md](./agent-poc/smolagents-codeagent-poc/文档.md)。

---

## 上下文文档（Agent 运行时读取）

- `docs/指标计算文档.md` — 标准字段定义 + 指标公式 + SQL 模板
- `docs/Agent.md`（待编写）— Agent 角色、工具链、安全约束

---

## 分支

当前分支：`agent-duckdb-pipeline`

