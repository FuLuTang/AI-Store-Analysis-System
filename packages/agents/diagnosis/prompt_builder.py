"""prompt_builder.py — 模块化拼合 system/user prompt。

每个返回字符串的小函数独立存在，join_parts() 控制拼接顺序。
调整提示词内容或顺序只需改这个文件，agent loop 不动。
"""

from datetime import datetime, timezone, timedelta

CST = timezone(timedelta(hours=8))


# ─────────────────────────────────────────────
# system content 拼合
# ─────────────────────────────────────────────

def build_system_content(analysis_params: str = "") -> str:
    return join_parts(
        identity(),
        security_rules(),
        system_analysis_params(analysis_params),
    )


def system_analysis_params(analysis_params: str) -> str:
    """系统级分析参数限制，使大模型全局遵守对比维度、语气和词藻风格限制。"""
    if not analysis_params or not analysis_params.strip():
        return ""
    return (
        "【分析约束与参数限制】\n"
        "本次数据分析中，你必须始终严格遵守以下用户自定义的参数设定和报告要求：\n"
        f"{analysis_params}"
    )


def identity() -> str:
    return (
        "你是一个自主经营数据分析 Agent。"
        "你可以通过调用工具来读取文件、查询 DuckDB 数据库、提取文档表格、执行 Python 脚本，最终产出完整的分析报告。"
        "你必须按计划逐步推进，每完成一步后调用 check_plan 验证并前进到下一步。"
        "当计划的全部步骤完成，或者遇到无法继续的严重错误时，必须调用 finish_task 结束/终止当前任务。"
    )


def security_rules() -> str:
    return (
        "安全规则：\n"
        "- 所有结论必须引用工具返回的真实数据，不得编造数值\n"
        "- duckdb_query 只能执行只读 SELECT 查询，禁止 INSERT/DROP/DELETE/ALTER\n"
        "- DuckDB 表名列名含中文或括号时需要双引号引用，如 SELECT \"毛利(元)\" FROM \"月营业数据\"，不要用反引号\n"
        "- Python 代码只能操作 workspace 内目录\n"
        "- 在 `scripts/old_session_scripts/` 目录下（如果存在）存有该用户最近几次运行生成的旧 Python 脚本（按 run_id 文件夹分类）。如果你发现有可复用的清洗/归一化脚本，你可以通过 `run_python` 直接执行它们（系统会自动将它们复制到你的根 `scripts/` 目录中）。注意：你没有直接修改或写入 `old_session_scripts/` 目录的权限。\n"
        "- 禁止结尾写客套话"
    )


# ─────────────────────────────────────────────
# user content 拼合
# ─────────────────────────────────────────────

def build_user_content(ws, analysis_params: str = "") -> str:
    return join_parts(
        current_time(),
        plan_progress(ws),
        workspace_summary(ws),
        user_analysis_params(analysis_params),
        task_instruction(),
    )


def user_analysis_params(analysis_params: str) -> str:
    """用户自定义分析参数，已在前端保存为清洗后的 KV 格式。"""
    if not analysis_params or not analysis_params.strip():
        return ""
    return f"【用户分析参数】\n{analysis_params}"


def current_time() -> str:
    now = datetime.now(CST)
    return f"当前时间：{now.strftime('%Y-%m-%d %H:%M')}（北京时间）"


def plan_progress(ws) -> str:
    """注入当前 plan 进度（简洁版，只显示当前 in_progress 步骤）。"""
    try:
        from ..core.tools.impl.setup_impl import read_plan_short_impl
        text = read_plan_short_impl(ws)
        return text if text else ""
    except Exception:
        return ""


def workspace_summary(ws) -> str:
    try:
        inputs = ws.list_inputs()
        tables = ws.list_parquet_files()
        lines = [f"工作区：{ws.dir}"]
        if inputs:
            lines.append(f"input 文件：{', '.join(inputs)}")
        if tables:
            lines.append(f"parquet 表：{', '.join(tables)}")
        return "\n".join(lines)
    except Exception:
        return ""


def task_instruction() -> str:
    return (
        "按 plan 中的步骤逐步推进。每完成一步后，必须调用 check_plan(step_index) 验证并前进到下一步。"
        "所有步骤全部完成后（或者遇到不可恢复的错误），调用 finish_task 结束任务。"
    )


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def join_parts(*parts: str) -> str:
    """拼接多个非空字符串段落，用双换行分隔。"""
    return "\n\n".join(p for p in parts if p and p.strip())
