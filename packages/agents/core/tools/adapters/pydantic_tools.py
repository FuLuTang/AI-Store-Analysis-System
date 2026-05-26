"""Pydantic AI adapter：@agent.tool + RunContext[Workspace] 模式"""
import json
import logging
from pydantic_ai import RunContext, Agent

from ...workspace import Workspace
from ...models import TableMeta

logger = logging.getLogger("agent.pydantic")


def _q(name: str) -> str:
    """如果表名没有被双引号包围且包含特殊字符，用双引号包围。"""
    if not name.startswith('"') and not name.endswith('"'):
        return f'"{name}"'
    return name


def register_pydantic_tools(agent: Agent, phase: str) -> Agent:
    # ---- 公共工具 ----

    @agent.tool
    async def profile_table_tool(ctx: RunContext[Workspace], table_name: str) -> TableMeta:
        """查看指定表的结构、列名、类型、样本值和行数。"""
        w = ctx.deps
        meta = (
            w.manifest.tables_by_name.get(table_name) or
            w.manifest.tables_by_duckdb_name.get(table_name)
        )
        if not meta:
            w.load_manifest()
            meta = (
                w.manifest.tables_by_name.get(table_name) or
                w.manifest.tables_by_duckdb_name.get(table_name)
            )
        if not meta:
            raise ValueError(f"表 {table_name!r} 不在 workspace 中（请用 SQL 表名如 '{table_name.replace('/', '_')}' 尝试）")
        logger.debug(f"[{phase}] profile_table_tool: {table_name}")
        return meta

    @agent.tool
    async def read_context_tool(ctx: RunContext[Workspace], topic: str) -> str:
        """读取上下文文档。topic: metrics/fields/pharmacy/restaurant/hr/common。"""
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
        doc_map = {
            "metrics": root / "docs" / "指标计算文档.md",
            "fields": root / "docs" / "指标计算文档.md",
            "pharmacy": root / "packages" / "domain_packs" / "pharmacy.yaml",
            "restaurant": root / "packages" / "domain_packs" / "restaurant.yaml",
            "hr": root / "packages" / "domain_packs" / "hr.yaml",
            "common": root / "packages" / "domain_packs" / "common.yaml",
        }
        path = doc_map.get(topic)
        content = path.read_text(encoding="utf-8") if path and path.is_file() else f"未找到文档: topic={topic}"
        logger.debug(f"[{phase}] read_context_tool: {topic} → {len(content)} chars")
        return content

    @agent.tool
    async def list_tables_tool(ctx: RunContext[Workspace]) -> list[dict]:
        """列出 workspace 中所有可用表的名称、SQL表名、行数和列名。"""
        w = ctx.deps
        w.load_manifest()
        result = []
        for t in w.manifest.tables:
            result.append({
                "name": t.name,
                "duckdb_name": t.duckdb_name,
                "row_count": t.row_count,
                "columns": [c.name for c in t.columns],
            })
        logger.debug(f"[{phase}] list_tables_tool: {len(result)} 张表")
        return result

    # ---- Flatten 阶段工具 ----

    if phase == "flatten":
        @agent.tool
        async def write_workspace_file(ctx: RunContext[Workspace], filename: str, content: str) -> str:
            """将内容写入 workspace 文件。"""
            p = ctx.deps.write_file(filename, content)
            logger.debug(f"[{phase}] write_file: {filename} ({len(content)} chars)")
            return f"已写入: {p}"

        @agent.tool
        async def run_python_tool(ctx: RunContext[Workspace], script_filename: str) -> str:
            """在 workspace 沙箱内执行指定 Python 脚本，返回 stdout。"""
            import subprocess
            import sys
            w = ctx.deps
            script_path = w.dir / script_filename
            if not script_path.is_file():
                return f"脚本不存在: {script_filename}"
            try:
                r = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True, text=True, timeout=30,
                )
                logger.debug(f"[{phase}] run_python: {script_filename} → {len(r.stdout)} chars")
                return r.stdout or r.stderr or "(no output)"
            except subprocess.TimeoutExpired:
                return "脚本执行超时 (30s)"

    # ---- Mapping 阶段工具 ----

    if phase == "mapping":
        @agent.tool
        async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
            """在 DuckDB 上执行 SELECT 查询并返回结果。"""
            import duckdb
            w = ctx.deps
            conn = duckdb.connect(w.duckdb_path)
            try:
                df = conn.execute(sql).fetchdf()
                logger.debug(f"[{phase}] duckdb_sql: {sql[:80]} → {len(df)} 行")
                return df.head(20).to_dict(orient="records")
            finally:
                conn.close()

    # ---- SQL 阶段工具 ----

    if phase == "sql":
        @agent.tool
        async def execute_duckdb_sql(ctx: RunContext[Workspace], sql: str) -> list[dict]:
            """在 DuckDB 上执行 SELECT 查询并返回结果。"""
            import duckdb
            w = ctx.deps
            conn = duckdb.connect(w.duckdb_path)
            try:
                df = conn.execute(sql).fetchdf()
                logger.debug(f"[{phase}] duckdb_sql: {sql[:80]} → {len(df)} 行")
                return df.head(20).to_dict(orient="records")
            finally:
                conn.close()

        @agent.tool
        async def register_parquet_tool(
            ctx: RunContext[Workspace], table_name: str, parquet_filename: str
        ) -> str:
            """将 workspace 中的 parquet 文件注册为 DuckDB 视图。"""
            import duckdb
            w = ctx.deps
            parquet_path = w.dir / parquet_filename
            if not parquet_path.is_file():
                return f"parquet 文件不存在: {parquet_filename}"
            qname = _q(table_name)
            conn = duckdb.connect(w.duckdb_path)
            try:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {qname} AS SELECT * FROM '{parquet_path}'"
                )
                logger.debug(f"[{phase}] register_parquet: {table_name} ← {parquet_filename}")
                return f"视图 {qname} 已注册"
            finally:
                conn.close()

        @agent.tool
        async def list_workspace_files(ctx: RunContext[Workspace]) -> list[str]:
            """列出 workspace 目录下所有文件。"""
            files = ctx.deps.list_files()
            logger.debug(f"[{phase}] list_files: {files}")
            return files

        @agent.tool
        async def validate_result_tool(ctx: RunContext[Workspace], result_json: str) -> str:
            """校验 AgentResult JSON 的完整性。"""
            errors_local = []
            try:
                data = json.loads(result_json)
                for field in ["mapping", "metrics", "warnings", "tables"]:
                    if not isinstance(data.get(field), list):
                        errors_local.append(f"{field} 必须是数组")
            except json.JSONDecodeError as e:
                errors_local.append(f"JSON 解析失败: {e}")
            msg = "校验通过" if not errors_local else "校验失败: " + "; ".join(errors_local)
            logger.debug(f"[{phase}] validate_result: {msg}")
            return msg

    return agent
