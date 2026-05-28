# Agent 方案

## 三条管线

三种方法实现同一个 `AgentPipeline` 接口，按场景切换：

| 方法 | 路径 | 定位 | 适用场景 |
|------|------|------|---------|
| 方法1 | 确定性管线 (`input_adapter → metric_engine`) | **线上默认**，稳定可靠 | 标准 JSON/Excel/CSV，字段规整 |
| 方法2 | Smolagents CodeAgent (`smol_pipeline.py`) | **脏数据处理**，动态写代码 | 深层嵌套、不规则结构、字段别名乱 |
| 方法3 | Pydantic AI (`pydantic_pipeline.py`) | **工程规范**，结构化 tool calling | 需要严格输入输出约束的场景 |

```python
if bundle.is_clean():
    pipeline = DeterministicPipeline()
elif config.agent_mode == "smolagents":
    pipeline = SmolPipeline()
else:
    pipeline = PydanticPipeline()

result = await pipeline.run(bundle)
```

三条管线实现同一 `AgentPipeline` 接口：

```python
class AgentPipeline:
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
```

### 共享包

```
packages/agents/
  ├── __init__.py            # 统一包入口，对外暴露通用接口
  ├── registry.py            # Pipeline 注册表
  ├── smol_pipeline.py       # [LEGACY] Smol 兼容管线
  ├── pydantic_pipeline.py   # [LEGACY] Pydantic 兼容管线
  ├── traditional_pipeline.py# [LEGACY] 传统兼容管线
  │
  ├── core/                  # 基础引擎核心包（通用）
  │     ├── __init__.py      # 核心模块入口
  │     ├── base.py          # Pipeline 基类
  │     ├── workspace.py     # 工作区生命周期
  │     ├── agent_loop.py    # Agent 运行主循环
  │     ├── tool_converter.py# OpenAI 工具格式转换
  │     ├── models.py        # 数据核心模型
  │     ├── logging_utils.py # 日志工具
  │     └── tools/           # 工具子包
  │           ├── __init__.py
  │           ├── impl/      # 纯函数底层实现层（file, python, sql...）
  │           └── adapters/  # 框架适配层（smol, pydantic 适配）
  │
  └── diagnosis/             # 业务服务：门店诊断（专属）
        ├── __init__.py      # 诊断服务入口
        ├── pipeline.py      # 诊断管线
        ├── prompt_builder.py# 诊断专属提示词拼装
        └── plan_template.py # 诊断专属的任务计划模板
  │
  ├── prompts/               # pydantic.md + smol.md 系统提示词
  └── tests/
```

### 路由

| 方法 | pipeline 文件 | 路由 |
|------|--------------|------|
| 方法1 | `packages/core/` (确定性管线) | `POST /api/analyze` |
| 方法2 | `packages/agents/smol_pipeline.py` | `POST /api/agent/analyze?pipeline=smol` |
| 方法3 | `packages/agents/pydantic_pipeline.py` | `POST /api/agent/analyze?pipeline=pydantic` |

---

## 动机

目前确定性管线（`input_adapter` → `metric_engine`）存在两个硬伤：

1. **输入格式脆弱**：深层嵌套 JSON、不规则结构直接歇菜
2. **字段映射硬编码**：`KEYWORD_MAP` 死字典，新行业/新字段需改代码

想做的：用 Agent + 工具调用（Python 执行 + SQL 执行），让 LLM 动态处理"看不懂"的数据。

---

## Agent 流程

```
数据输入（任意格式）
  ↓ Workspace: write_raw_parquet → init_duckdb
  ↓ design_plan: 写入任务清单 plan.json
  ↓ CodeAgent: 写 Python 展平 → DuckDB SQL 计算 → 输出 AgentResult
  ↓ AgentResult (scene + mapping + metrics + full_report + cards)
```

---

## 技术边界

- **后端语言**：Python（现有 FastAPI 项目）
- **LLM 端点**：已有 OpenAI-compatible API（deepseek），支持 function calling
- **SQL 引擎**：DuckDB（零依赖，单进程，原生支持 pandas/parquet/JSON）
- **任务类型**：单 Agent 完成全流程（展平→画像→映射→计算→输出），最多 15 轮迭代

---

## 上下文文档（Agent 运行时读取）

- `docs/指标计算文档.md` — 标准字段定义 + 指标公式 + SQL 模板
- `docs/Agent.md`（待编写）— Agent 角色、工具链、安全约束

---

## 分支

当前分支：`agent-smolagents`

