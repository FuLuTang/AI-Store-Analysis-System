# Agent Tools设计

本文档描述当前诊断 Agent 的工具设计现状、已完成改造、待完成项，以及目标形态。

当前默认语境：
- 状况分析服务最终只保留 `custom` agent
- 工具由 `packages/agents/core/tool_converter.py` 暴露给 LLM
- 工具底层实现位于 `packages/agents/core/tools/impl/`

## 1. 设计目标

工具设计的核心目标有四个：

1. 避免把大文件全文塞进 LLM 上下文。
2. 把“文件侦察”和“内容精读”分开。
3. 把“结构化查询”和“复杂处理”分开。
4. 让工具返回结构尽量稳定，便于 Agent 预测下一步动作。

因此不建议做一个“万能文件工具”，而应保留少量、边界清晰的工具。

## 2. 当前工具架构

当前 custom agent 的工具入口分三层：

1. 工具 schema  
位置：`packages/agents/core/tool_converter.py`  
作用：定义 tool name、description、parameters，直接传给 LLM 的 function calling。

2. 工具映射  
位置：`packages/agents/core/tool_converter.py` 的 `build_tool_map()`  
作用：把工具名映射到具体 Python callable。

3. 工具实现  
位置：`packages/agents/core/tools/impl/`  
作用：真正执行文件读取、脚本运行、DuckDB 查询等行为。

## 3. 已完成

### 3.1 `list_files(subdir="")`

当前已实现：
- 列出目录下文件
- 返回 `path`、`name`、`size`、`size_human`

当前返回示例：

```json
[
  {
    "path": "input/a.txt",
    "name": "a.txt",
    "size": 1234,
    "size_human": "1.2KB"
  }
]
```

当前用途：
- 先侦察输入规模
- 让 Agent 决定后续用 `read_file`、`read_document` 还是 `run_python`

### 3.2 `read_file(path, offset=0, limit=2000)`

当前已实现：
- 分页读取文本文件
- 单次最多 2000 行
- 单次输出有字节上限
- 返回 `next_offset`
- 对疑似二进制文件直接拒绝全文读取

当前返回示例：

```json
{
  "path": "output/a.txt",
  "size": 19,
  "size_human": "19B",
  "offset": 1,
  "limit": 2,
  "line_start": 1,
  "line_end": 3,
  "total_lines": 4,
  "has_more": true,
  "next_offset": 3,
  "truncated": true,
  "content": "two\nthree\n",
  "note": "文件未读完，请用 next_offset 继续分页读取。"
}
```

当前定位：
- 用于文本精读
- 适合脚本、日志、Markdown、输出 JSON、小文本配置
- 不适合 zip/sqlite/xlsx/pdf/big-json 全文读取

### 3.3 `write_file(path, content, mode="overwrite")`

当前已实现：
- `overwrite` 原子覆盖
- `append` 追加
- 返回写入后的大小信息

当前返回示例：

```json
{
  "ok": true,
  "path": "output/result.json",
  "mode": "overwrite",
  "bytes_written": 128,
  "size": 128,
  "size_human": "128B"
}
```

当前定位：
- 产物写入
- 中间结果落盘
- 文本报告生成

### 3.4 `read_document(path)`

当前已实现：
- `.xlsx/.xls`：sheet、列名、样本
- `.csv`：列名、样本
- `.pdf`：页数、页摘要、表格数量
- `.docx`：段落和表格摘要

当前问题：
- 名字叫 `read_document`，容易让 Agent 误解成“读全文”
- `.txt/.md/.json` 当前仍可能全文返回
- 部分实现仍有全量读入内存的风险
- 不同格式返回结构不统一

当前定位应视为：
- 文档结构侦察工具
- 不是全文读取工具

### 3.5 `run_python(script_path)`

当前已实现：
- 在 workspace 下执行 `scripts/*.py`
- 返回 stdout / stderr

当前适合处理：
- 大文件
- 压缩包
- sqlite
- 复杂 Excel / CSV 清洗
- 自定义格式转换

当前环境能力：
- Python 标准库可用，包括 `sqlite3`
- `pandas`
- `duckdb`
- `openpyxl`
- `pdfplumber`
- `python-docx`

### 3.6 `duckdb_query(sql)`

当前已实现：
- 在 workspace 的 DuckDB 上执行只读查询
- 用于 parquet / DuckDB 表上的结构化分析

当前定位：
- 指标计算
- 小范围验证
- 已整理数据的结构化探索

## 4. 已做但仍需继续收口

### 4.1 分析服务只保留 custom

目标状态：
- 状况分析服务只保留 `custom` agent
- 前端不再提供 pipeline 切换
- 后端不再维护 `traditional` / `pydantic` / `smol` 三条历史管线

这部分需要与实际代码状态保持同步。

### 4.2 提示词已开始约束工具使用方式

当前诊断 plan 已引导：
- 先 `list_files`
- 再 `read_document`
- 小文本用 `read_file`
- 大文件或复杂格式用 `run_python`

这一步是对的，但仅靠 prompt 不够，工具本身仍需要严格边界。

## 5. 待做

### 5.1 `search_files(pattern, path=None, regex=false, max_matches=50)`

建议优先级：最高

目标：
- 先搜索，再局部读取
- 避免大日志、大 JSON、大 Markdown 被直接全文塞进上下文

建议返回：

```json
[
  {
    "path": "input/a.json",
    "total_matches": 2,
    "matches": [
      {"line": 31, "text": "\"会员数\": 123"},
      {"line": 88, "text": "\"会员数\": 456"}
    ]
  }
]
```

设计建议：
- 默认普通子串搜索
- 可选 `regex=true`
- 可选指定单文件 `path`
- 命中后再用 `read_file(offset=...)` 读取上下文

说明：
- 工具名建议叫 `search_files`
- 不建议直接叫 `grep`
- 内部行为可以借鉴 `grep`

### 5.2 `read_document_structure(path)`

建议优先级：高

目标：
- 把当前 `read_document` 演进成统一的“结构侦察工具”
- 不再做全文返回
- 不同格式统一输出 JSON

建议支持：
- `.md`：标题层级、行号、强调文本摘要
- `.json`：顶层 key、数组长度、嵌套路径、样本结构
- `.csv`：列名、样本、行数
- `.xlsx`：sheet、列名、样本
- `.pdf`：页数、页摘要、表格概览
- `.zip`：压缩包内文件清单
- `.sqlite`：表名、schema、样本

建议返回形态：

```json
{
  "path": "input/report.xlsx",
  "kind": "xlsx",
  "size": 1048576,
  "size_human": "1.0MB",
  "summary": {
    "sheets": [
      {
        "name": "Sheet1",
        "columns": ["日期", "销售额", "毛利"],
        "sample_rows": [
          {"日期": "2026-05-01", "销售额": 1234, "毛利": 321}
        ]
      }
    ]
  },
  "recommended_next_tool": "run_python"
}
```

### 5.3 `read_file` 增加 `head` / `tail`

建议优先级：中

目标：
- 支持看文件开头
- 支持看文件结尾
- 适合日志、Markdown、脚本快速扫读

建议参数：

```python
read_file(path, offset=0, limit=2000, head=None, tail=None)
```

建议规则：
- `offset/limit`：读中间片段
- `head`：读前 N 行
- `tail`：读后 N 行
- `head + tail`：读首尾，中间省略

### 5.4 `query_sqlite(path, sql)`

建议优先级：中

目标：
- 给 sqlite 提供专用只读查询工具

为什么仍有价值：
- 虽然 `run_python` 已能用 `sqlite3`
- 但专用工具更容易被 Agent 正确使用
- 更容易统一限制只读、限制结果大小、统一返回 JSON

建议规则：
- 只允许 `SELECT`
- 自动或强制 `LIMIT`
- 返回行数上限

### 5.5 `list_files` 增强字段

建议优先级：中

建议增加：
- `ext`
- `kind`
- `recommended_tool`

目标：
- 让 Agent 在第一步就更容易做出正确工具选择

## 6. 不建议优先拆出的工具

### 6.1 `inspect_archive(path)`

当前不建议单独拆出。

原因：
- 若 `read_document_structure` 已支持 zip/rar/7z
- 则 archive 侦察可以并入统一结构工具

### 6.2 `extract_document_tables(path, sheet, limit)`

当前不建议优先暴露。

原因：
- 和 `run_python`、`duckdb_query`、未来的结构工具有重复
- 如果未来 Agent 确实频繁需要“小批量抽表格”，再单独加也不迟

## 7. 最终工具集合建议

推荐最终保留的核心工具集合：

1. `list_files`
2. `read_file`
3. `write_file`
4. `search_files`
5. `read_document_structure`
6. `run_python`
7. `duckdb_query`

可选增强：

1. `query_sqlite`

不建议继续扩展成很多互相重叠的小工具。

原则：
- 文件侦察：`list_files`
- 文本定位：`search_files`
- 文本精读：`read_file`
- 结构侦察：`read_document_structure`
- 复杂处理：`run_python`
- 结构化查询：`duckdb_query` / `query_sqlite`

## 8. 推荐实现顺序

如果按性价比排序，建议顺序如下：

1. `search_files`
2. `read_document -> read_document_structure`
3. `read_file` 增加 `head/tail`
4. `list_files` 增强 `kind/recommended_tool`
5. `query_sqlite`

如果只能先做一项，优先做 `search_files`。

原因：
- 它能立刻降低大文本误读概率
- 并且和当前已经完成分页化的 `read_file` 天然互补
