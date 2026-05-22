"""Smolagents adapter：@tool + 闭包捕获 workspace"""
import json
from ..workspace import Workspace
from ..tools.impl.file_impl import read_file_impl, write_file_impl, list_files_impl
from ..tools.impl.python_impl import run_python_impl
from ..tools.impl.duckdb_impl import duckdb_query_impl, duckdb_register_parquet_impl
from ..tools.impl.context_impl import read_context_impl
from ..tools.impl.profile_impl import profile_table_impl
from ..tools.impl.validate_impl import validate_result_impl
from ..tools.impl.setup_impl import setup_workspace_impl, cleanup_workspace_impl, list_tables_impl
from ..tools.impl.doc_impl import read_document_impl, extract_document_tables_impl


def build_smol_tools(ws: Workspace, emit_log=None):
    try:
        from smolagents import tool
    except ImportError:
        raise ImportError("smolagents 未安装，请执行: pip install smolagents")

    @tool
    def read_file(path: str) -> str:
        """Read a file from the workspace.

        Args:
            path: Relative path in workspace, e.g. 'input/overview.json' or 'output/result.json'.
        """
        return read_file_impl(ws, path)

    @tool
    def write_file(path: str, content: str) -> str:
        """Write content to a file in the workspace.

        Args:
            path: Relative path in workspace, e.g. 'scripts/flatten.py' or 'output/result.json'.
            content: Text content to write.
        """
        return write_file_impl(ws, path, content)

    @tool
    def list_files(subdir: str = "") -> list[str]:
        """List files in a workspace subdirectory.

        Args:
            subdir: Subdirectory to list, e.g. 'input', 'tables', 'output'. Empty = root.
        """
        return list_files_impl(ws, subdir)

    @tool
    def run_python(script_path: str) -> str:
        """Execute a Python script inside the workspace sandbox.

        Args:
            script_path: Relative path to the script, e.g. 'scripts/flatten.py'.
        """
        return run_python_impl(ws, script_path)

    @tool
    def duckdb_query(sql: str) -> str:
        """Execute a read-only SQL query on the workspace DuckDB database. Returns JSON.

        Args:
            sql: SELECT-only SQL query. Do NOT use DROP/DELETE/INSERT/ALTER.
        """
        return duckdb_query_impl(ws, sql)

    @tool
    def duckdb_register_parquet(table_name: str, parquet_path: str) -> str:
        """Register a parquet file as a DuckDB table (CREATE VIEW).

        Args:
            table_name: Table name exposed to DuckDB, e.g. 'sales_flat'.
            parquet_path: Workspace-relative path, e.g. 'tables/sales_flat.parquet'.
        """
        return duckdb_register_parquet_impl(ws, table_name, parquet_path)

    @tool
    def read_context(doc_name: str) -> str:
        """Read a context document injected into workspace context/ directory.

        Args:
            doc_name: Document file name, e.g. '指标计算文档.md'.
        """
        return read_context_impl(ws, doc_name)

    @tool
    def profile_table(parquet_path: str) -> str:
        """Profile a parquet file: returns column names, dtypes, sample values, null rates.

        Args:
            parquet_path: Workspace-relative path, e.g. 'tables/sales_flat.parquet'.
        """
        return profile_table_impl(ws, parquet_path)

    @tool
    def validate_result(raw: str) -> str:
        """Validate output JSON against AgentResult schema before submission.

        Args:
            raw: JSON string of the AgentResult to validate.
        """
        data = json.loads(raw) if isinstance(raw, str) else raw
        return str(validate_result_impl(data))

    @tool
    def setup_workspace() -> str:
        """Initialize workspace: scan parquet files, register all as DuckDB views, return status summary. Call once at start."""
        return setup_workspace_impl(ws)

    @tool
    def cleanup_workspace(mode: str = "large") -> str:
        """Clean up workspace files. Call after saving output/result.json.

        Args:
            mode: 'large' to delete parquet+duckdb only (keeps trace/scripts), 'all' to delete everything.
        """
        return cleanup_workspace_impl(ws, mode)

    @tool
    def list_tables() -> str:
        """List all registered DuckDB tables and their names."""
        return list_tables_impl(ws)

    @tool
    def read_plan() -> str:
        """Read the full task plan from output/plan.json with all step details.
        """
        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return json.dumps({"error": "plan.json not found"})
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        for step in plan:
            step.pop("check", None)
        return json.dumps(plan, ensure_ascii=False, indent=2)

    @tool
    def check_plan(step_index: int) -> str:
        """【必须调用】完成 plan 当前步骤后必须调此工具来验证产物并推进到下一步。
        不调的话 plan 状态不会更新，你永远看不到下一步的指令。

        每完成 plan 中的一个步骤后，先调本工具验证，通过后系统会自动进入下一步。

        Args:
            step_index: plan 中步骤的 0-based 索引。
        """
        from ..tools.impl.plan_check_impl import run_step_check

        plan_path = ws.resolve("output/plan.json")
        if not plan_path.exists():
            return json.dumps({"error": "plan.json not found"})
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        if step_index < 0 or step_index >= len(plan):
            return json.dumps({"error": f"step_index {step_index} out of range (0-{len(plan)-1})"})

        step = plan[step_index]
        ok, errors = run_step_check(ws, step)
        step["errors"] = errors
        step["status"] = "success" if ok else "failed"

        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

        if ok:
            for i in range(step_index + 1, len(plan)):
                if plan[i]["status"] == "pending":
                    plan[i]["status"] = "in_progress"
                    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
                    if emit_log:
                        emit_log("smol_plan", f"Step {i} 开始: {plan[i]['title']}")
                    break

        if emit_log:
            status_icon = "✓" if ok else "✗"
            emit_log("smol_plan", f"Step {step_index} {status_icon}: {step['title']}")

        result = {
            "step_index": step_index,
            "ok": ok,
            "errors": errors,
        }
        if errors:
            result["next_action"] = (
                f"Step {step_index}（{step['title']}）未通过检查，请根据报错排查后重新 check_plan({step_index})\n"
                + "\n".join(errors)
            )
        return json.dumps(result, ensure_ascii=False, indent=2)

    @tool
    def read_document(path: str) -> str:
        """Read the full text content from a document. Auto-detects format from file extension.

        Supports xlsx, csv, pdf, docx, txt, md, json.
        For xlsx: returns sheet names, column headers, row count, and sample rows per sheet.
        For pdf/docx: returns extracted text and a summary of tables found.
        For txt/md/json: returns raw text content.

        Args:
            path: Relative path in workspace, e.g. 'input/report.pdf'.
        """
        return read_document_impl(ws, path)

    @tool
    def extract_document_tables(path: str, sheet: str = "") -> str:
        """Extract structured table data from a document as JSON rows.

        Supports xlsx, csv, pdf, docx.
        For xlsx: extracts the given sheet (or first sheet if not specified).
        For csv: extracts all rows.
        For pdf: extracts tables from all pages.
        For docx: extracts tables from the document.
        Each row is a dict with column names as keys.

        Args:
            path: Relative path in workspace, e.g. 'input/sales.xlsx'.
            sheet: (Optional) Sheet name to extract. Case-insensitive partial match. Leave empty for auto/first.
        """
        return extract_document_tables_impl(ws, path, sheet)

    return [
        read_file, write_file, list_files,
        run_python,
        duckdb_query, duckdb_register_parquet,
        read_context,
        profile_table,
        validate_result,
        setup_workspace, list_tables,
        read_plan, check_plan,
        read_document, extract_document_tables,
    ]
