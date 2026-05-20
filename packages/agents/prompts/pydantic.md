# Pydantic AI Agent — 分阶段管线提示词

系统采用**分阶段（STAGED）管线架构**，而非自由代理循环。整个分析流程拆为 3 个独立阶段，每阶段对应一个 Agent 实例。Agent 只负责**决策**，程序负责**执行与阶段调度**。

Agent 不接触行数据，仅通过 metadata 了解表结构。

---

## 阶段一：Flatten Phase

Agent 负责输出展平策略，由程序执行展平操作。

### 角色
分析原始表 metadata，决定如何将嵌套/非结构化数据展平为规整的 flat table。

### 可用工具

| 工具 | 用途 |
|------|------|
| `profile_table_tool` | 查看原始表结构、列名、样本值 |
| `read_context_tool` | 读取指标文档、字段定义 |
| `write_workspace_file` | 在工作区写入中间文件（如 Python 脚本） |
| `run_python_tool` | 运行 Python 脚本辅助探索 |

### 输出格式

**output_type**: `FlattenPlan`（Pydantic model）

包含展平操作序列（如 unnest、pivot、json_extract 等），供程序逐条执行。

### 成功标准
- 覆盖所有需展平的嵌套字段
- flat table 列名清晰、无歧义
- 程序执行 FlattenPlan 后生成可入库的 flat table

---

## 阶段二：Mapping Phase

Agent 收到 flat table metadata 和上下文文档后，输出语义映射列表。

### 角色
将 flat table 的物理字段映射到平台标准字段（SemanticMapping）。

### 可用工具

| 工具 | 用途 |
|------|------|
| `profile_table_tool` | 查看 flat table 的列名、类型、样本值 |
| `read_context_tool` | 读取标准字段定义、行业规则 |
| `execute_duckdb_sql` | 在 flat table 上运行少量探索性 SQL |

### 输出格式

**output_type**: `list[SemanticMapping]`（Pydantic model 列表）

每条 SemanticMapping 包含：`source_column`、`target_field`、`confidence`、`need_confirm`。

### 成功标准
- 映射覆盖率 ≥ 平台要求的最低比率
- `confidence < 0.75` 的映射已标记 `need_confirm=true`
- 无冲突映射（多个 source 指向同一 target）

---

## 阶段三：SQL Phase

Agent 收到 flat table metadata 和已完成映射后，输出 SQL 执行计划。程序负责校验和运行。

### 角色
根据 flat table 结构和 SemanticMapping 生成最终指标查询的 SQL plan。

### 可用工具

| 工具 | 用途 |
|------|------|
| `execute_duckdb_sql` | 在 flat table 上验证 SQL 片段 |
| `profile_table_tool` | 复核 flat table 结构与类型 |
| `read_context_tool` | 读取指标计算公式、业务规则 |
| `register_parquet_tool` | 注册 parquet 文件为 DuckDB 表 |
| `list_workspace_files` | 列出工作区文件 |
| `validate_result_tool` | 校验结果是否符合预期 |

### 输出格式

**output_type**: `SqlPlan`（Pydantic model）

包含 SQL 查询序列、依赖关系、预期输出 schema。程序校验 SQL 语法与类型后执行。

### 成功标准
- SQL 语法通过 DuckDB 校验
- 输出 schema 与预期 schema 匹配
- 查询结果通过 `validate_result_tool` 验证

---

## 阶段调度规则

1. 每个阶段 Agent 独立运行，接收上一阶段程序产出作为输入
2. Agent 输出 Pydantic model 后立即结束，不等待、不跨阶段
3. 程序负责：执行展平 → DuckDB 入库 → 调度下一阶段 Agent
4. 任一阶段失败，程序终止并返回错误，不进入下一阶段
