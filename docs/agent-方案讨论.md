# Agent 方案

## 三条管线

三种方法实现同一个 `AgentPipeline` 接口，按场景切换：

| 方法 | 路径 | 定位 | 适用场景 |
|------|------|------|---------|
| 方法1 | 确定性管线 (`input_adapter → metric_engine`) | **线上默认**，稳定可靠 | 标准 JSON/Excel/CSV，字段规整 |
| 方法2 | Smolagents CodeAgent (`agent-smolagents`) | **脏数据处理**，动态写代码 | 深层嵌套、不规则结构、字段别名乱 |
| 方法3 | Pydantic AI (`agent-pydantic`) | **工程规范**，结构化 tool calling | 需要严格输入输出约束的场景 |

```python
if bundle.is_clean():
    pipeline = DeterministicPipeline()
elif config.agent_mode == "smolagents":
    pipeline = SmolagentPipeline()
else:
    pipeline = PydanticAgentPipeline()

result = await pipeline.run(bundle)
```

三条管线实现同一 `AgentPipeline` 接口：

```python
class AgentPipeline:
    async def run(self, bundle: DatasetBundle) -> AgentResult:
        ...
```

### 共享文件

```
packages/agents/
  ├── models.py              # AgentResult, DatasetBundle, RawTable, MetricResult
  ├── base.py                # AgentPipeline 抽象接口
  ├── workspace.py           # AgentWorkspace (独立临时目录)
  └── tools/                 # 6 个共享工具
```

三条管线分别在各自的 pipeline 文件和路由中实现：

| 方法 | pipeline 文件 | 路由 |
|------|--------------|------|
| 方法1 | `packages/core/` (确定性管线) | `POST /api/analyze` |
| 方法2 | `packages/agents/smol_pipeline.py` | `POST /api/agent/analyze?pipeline=smol` |
| 方法3 | `packages/agents/pydantic_pipeline.py` | `POST /api/agent/analyze?pipeline=pydantic` |

---

## 上下文文档（Agent 运行时读取）

- `docs/指标计算文档.md` — 标准字段定义 + 指标公式 + SQL 模板
- `docs/Agent.md`（待编写）— Agent 角色、工具链、安全约束

---

## 分支

当前分支：`agent-duckdb-pipeline`

