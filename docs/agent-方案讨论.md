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
  ├── models.py              # AgentResult, DatasetBundle, FlattenPlan, SemanticMapping, SqlPlan, ...
  ├── base.py                # AgentPipeline 抽象接口
  ├── workspace.py           # Workspace (独立临时目录 + parquet + manifest)
  ├── tools/                 # 9 个共享工具 (file/duckdb/python/context/validate/profile)
  ├── prompts/               # pydantic.md + smol.md 系统提示词
  └── tests/                 # 单元测试
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
  ↓ Agent: Python Tool — 写代码解析 & 展平为二维表
  ↓ Agent: 写入 DuckDB — 结构化存查
  ↓ Agent: 读上下文文档 — 匹配标准字段名
  ↓ Agent: SQL Tool — 写 SQL 算指标
  ↓ [确定性层: Threshold → Evidence → LLM Report Writer]
```

---

## 技术边界

- **后端语言**：Python（现有 FastAPI 项目）
- **LLM 端点**：已有 OpenAI-compatible API（deepseek），支持 function calling
- **SQL 引擎**：DuckDB（零依赖，单进程，原生支持 pandas/parquet/JSON）
- **任务类型**：单 Agent 序列执行，每次调用完成一步

---

## 上下文文档（Agent 运行时读取）

- `docs/指标计算文档.md` — 标准字段定义 + 指标公式 + SQL 模板
- `docs/Agent.md`（待编写）— Agent 角色、工具链、安全约束

---

## 分支

当前分支：`agent-duckdb-pipeline`

