"""Programmatic workflow for the first price recommendation implementation."""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, Union

from .precheck import (
    build_price_point_artifacts,
    inspect_uploaded_files,
)
from .data_fitting import run_data_fitting

logger = logging.getLogger("price_recommendation")

LogCallback = Callable[[str, dict], None]
AbortCallback = Callable[[], None]


def run_price_recommendation_workflow(
    *,
    decoded_files: list[dict],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path,
    llm_preset: dict,
    emit_log: LogCallback,
    check_aborted: AbortCallback | None = None,
) -> tuple[dict, str]:
    """Run the price workflow with Agent and write final fitted artifacts."""
    import copy
    from openai import OpenAI
    from packages.agents.core.workspace import Workspace
    from packages.agents.core.agent_loop import AgentLoop
    from packages.agents.core.tools.impl.setup_impl import design_plan_impl, read_plan_short_impl
    from packages.agents.core.tools.impl.file_impl import list_files_impl
    from packages.agents.core.tools.impl.doc_impl import read_document_structure_impl
    from packages.agents.price_recommendation.plan_template import PRICE_PLAN_TEMPLATE
    from packages.agents.core.tool_converter import get_plan_progress_info

    ws = Workspace(base_dir=workspace_dir)

    logger.info("Initialized workspace at %s", workspace_dir)

    def checkpoint():
        if check_aborted:
            check_aborted()

    emit_log("price_init", {"level": "info", "message": "价格推荐任务初始化完成", "progress": 5})
    checkpoint()

    # Save original files and unpack
    for item in decoded_files:
        filename = _safe_filename(item.get("name") or "unnamed")
        ws.write_input(filename, item.get("bytes") or b"")
    ws.unpack_archives()
    logger.info("Unpacked %d input files", len(decoded_files))
    checkpoint()

    # ── 写入 plan ──
    plan = copy.deepcopy(PRICE_PLAN_TEMPLATE)
    design_plan_impl(ws, json.dumps(plan, ensure_ascii=False))
    
    plan_path = ws.resolve("plan.json")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if plan and plan[0]["status"] == "pending":
        plan[0]["status"] = "in_progress"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    checkpoint()

    # ── 预探索：Agent 启动前分析数据 ──
    emit_log("price_parse", {"level": "info", "message": "正在进行数据预探索...", "progress": 10})
    pre_explore_text = _run_pre_exploration(ws, product_name)

    # ── 构建 client ──
    preset = llm_preset or {}
    api_key = preset.get("apiKey", "")
    base_url = preset.get("baseUrl", "https://api.deepseek.com")
    client = OpenAI(api_key=api_key, base_url=base_url, max_retries=0)

    def bootstrap_emit_log(node_id: str, message: Union[str, dict]):
        emit_log("price_parse", message)

    bootstrap_messages = _build_bootstrap_messages(
        ws=ws,
        plan_short_text=read_plan_short_impl(ws, emit_log=bootstrap_emit_log),
        list_files_json=json.dumps(list_files_impl(ws, "", emit_log=bootstrap_emit_log), ensure_ascii=False),
        read_document_structure_impl=read_document_structure_impl,
        emit_log=bootstrap_emit_log,
        pre_explore_text=pre_explore_text,
    )

    # Log initial parse
    emit_log("price_parse", {"level": "status", "message": "正在解析上传文件...", "progress": 18})

    # ── Log Proxy ──
    def mapped_emit_log(node_id: str, message: Union[str, dict]):
        curr_idx, total_steps = get_plan_progress_info(ws)
        target_node = node_id
        if node_id == "custom_agent":
            if curr_idx == 0:
                progress = 15
                if isinstance(message, dict):
                    progress = message.get("progress", 15)
                if progress < 20:
                    target_node = "price_parse"
                elif progress < 30:
                    target_node = "price_product_match"
                else:
                    target_node = "price_field_mapping"
            elif curr_idx == 1:
                target_node = "price_db"
            elif curr_idx == 2:
                target_node = "price_normalize"
            else:
                target_node = "price_normalize"
        elif node_id == "plan":
            if curr_idx == 1:
                target_node = "price_db"
            elif curr_idx == 2:
                target_node = "price_normalize"

        # Forward the log to standard callback
        emit_log(target_node, message)

    # ── 运行 Agent Loop ──
    loop = AgentLoop(
        client=client,
        ws=ws,
        llm_preset=preset,
        emit_log=mapped_emit_log,
        emit_status=None,
        check_aborted=check_aborted,
        bootstrap_messages=bootstrap_messages,
        task_type="price_recommendation",
        product_name=product_name,
        candidate_count=candidate_count,
    )

    try:
        loop.run()
    except Exception as e:
        logger.error("Agent loop failed: %s", str(e), exc_info=True)
        emit_log("price_done", {"level": "error", "message": f"价格推荐任务失败: {str(e)}", "error_details": str(e)})
        raise e

    checkpoint()

    if not _is_plan_done(ws):
        logger.error("Agent plan not fully completed")
        raise RuntimeError("Agent 价格推荐清洗与归一化步骤未全部完成")

    logger.info("Agent plan completed, starting data fitting")
    # ── 开始数据拟合与连续搜索 ──
    emit_log("price_curve_fit", {
        "level": "status",
        "message": "正在进行数据拟合与连续曲线生成...",
        "progress": 86,
    })
    
    norm_path = ws.resolve("output/normalized_price_points.json")
    if not norm_path.exists():
        logger.error("normalized_price_points.json not found at %s", norm_path)
        raise FileNotFoundError("未找到归一化价格点文件 (output/normalized_price_points.json)")
    
    normalized_payload = json.loads(norm_path.read_text(encoding="utf-8"))
    
    raw_path = ws.resolve("output/raw_price_points.json")
    raw_points = []
    if raw_path.exists():
        try:
            raw_data = json.loads(raw_path.read_text(encoding="utf-8"))
            if isinstance(raw_data, dict):
                raw_points = raw_data.get("rawPoints", raw_data.get("points", []))
            elif isinstance(raw_data, list):
                raw_points = raw_data
        except Exception:
            pass

    evidence = {
        "matchedRows": len(raw_points),
        "rawPointCount": len(raw_points),
        "storeField": "shop",
        "sourceTables": ws.list_inputs(),
        "notes": ["基于 Agent 清洗和归一化的价格点集进行数据拟合"],
    }

    emit_log("price_recommend", {
        "level": "status",
        "message": "正在分析最优价格推荐...",
        "progress": 90,
    })
    
    logger.info("Running data fitting with %d raw price points", len(raw_points))
    result = run_data_fitting(
        normalized_payload=normalized_payload,
        evidence=evidence,
        product_name=product_name,
        candidate_count=candidate_count,
        workspace_dir=ws.dir,
    )
    logger.info("Data fitting completed, best_price=%s, recommendations=%d",
                result.get("bestPrice"), len(result.get("recommendations", [])))
    checkpoint()

    emit_log("price_validate", {
        "level": "status",
        "message": "正在校验推荐结果 JSON...",
        "progress": 94,
    })
    _validate_result(result)
    
    _write_json(ws.output_dir / "price_recommendation.json", result)
    summary = _build_summary(result)
    (ws.dir / "summary.md").write_text(summary, encoding="utf-8")
    
    emit_log("price_done", {"level": "info", "message": "价格推荐任务完成", "progress": 100})
    logger.info("Price recommendation workflow completed successfully")
    return result, summary


def _is_plan_done(ws: Workspace) -> bool:
    try:
        plan_path = ws.resolve("plan.json")
        if not plan_path.exists():
            return False
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        return all(s.get("status") == "success" for s in plan)
    except Exception:
        return False


def _validate_result(result: dict):
    if result.get("taskType") != "price_recommendation":
        raise ValueError("结果 taskType 必须为 price_recommendation")
    recommendations = result.get("recommendations")
    if not isinstance(recommendations, list) or not recommendations:
        raise ValueError("结果缺少 recommendations")
    normalized_points = result.get("normalizedPoints")
    if not isinstance(normalized_points, list) or not normalized_points:
        raise ValueError("结果缺少 normalizedPoints")
    rendered_final_charts = result.get("renderedFinalCharts")
    if not isinstance(rendered_final_charts, list) or not rendered_final_charts:
        raise ValueError("结果缺少 renderedFinalCharts")
    for item in recommendations:
        if not isinstance(item.get("price"), (int, float)) or item["price"] <= 0:
            raise ValueError("推荐价格必须为正数")


def _build_summary(result: dict) -> str:
    lines = [
        "# 最优价格推荐结果",
        "",
        f"商品名称：{result.get('productName', '')}",
        "",
        "## 推荐价格",
    ]
    for item in result.get("recommendations", []):
        lines.append(f"- 第 {item.get('rank')} 推荐：{item.get('price')} {item.get('unit', '元')}")
    evidence = result.get("evidence", {})
    lines.extend([
        "",
        "## 数据证据",
        f"- 命中行数：{evidence.get('matchedRows', 0)}",
        f"- 归一化价格点：{len(result.get('normalizedPoints', []))}",
        f"- 时间颗粒度：{result.get('timeGranularity', result.get('dataFitting', {}).get('timeGranularity', '未指定'))}",
        f"- 渲染图表数：{len(result.get('renderedFinalCharts', []))}",
        f"- 最优售价：{result.get('bestPrice', '')}",
        "",
        "当前版本先完成归一化点集，再接数据拟合层输出渲染图表和推荐售价。",
    ])
    return "\n".join(lines)


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/]+", "_", name).strip()
    return cleaned or "unnamed"


def _write_analysis_duckdb(path: Path, raw_points: list[dict], normalized_points: list[dict]):
    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE IF EXISTS raw_price_points")
        con.execute(
            """
            CREATE TABLE raw_price_points (
              price DOUBLE,
              raw_qty DOUBLE,
              source_shop VARCHAR,
              source_table VARCHAR,
              source_date VARCHAR
            )
            """
        )
        if raw_points:
            con.executemany(
                "INSERT INTO raw_price_points VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        item.get("price"),
                        item.get("rawQty"),
                        item.get("sourceShop"),
                        item.get("sourceTable"),
                        item.get("sourceDate"),
                    )
                    for item in raw_points
                ],
            )

        con.execute("DROP TABLE IF EXISTS normalized_price_points")
        con.execute(
            """
            CREATE TABLE normalized_price_points (
              price DOUBLE,
              raw_qty DOUBLE,
              normalized_qty DOUBLE,
              sample_count INTEGER,
              avg_factor DOUBLE,
              source_shops_json VARCHAR
            )
            """
        )
        if normalized_points:
            con.executemany(
                "INSERT INTO normalized_price_points VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        item.get("price"),
                        item.get("rawQty"),
                        item.get("normalizedQty"),
                        item.get("sampleCount"),
                        item.get("avgFactor"),
                        json.dumps(item.get("sourceShops", []), ensure_ascii=False),
                    )
                    for item in normalized_points
                ],
            )
    finally:
        con.close()


def _run_pre_exploration(ws, product_name: str) -> str:
    """Run data pre-exploration before Agent starts, return printed summary."""
    script = (
        'import csv, json, io, sys, os\n'
        'from collections import Counter\n'
        'from pathlib import Path\n'
        '\n'
        'inputs = os.listdir("input")\n'
        'if not inputs:\n'
        '    print("{\\"error\\": \\"input 目录为空\\"}")\n'
        '    sys.exit(0)\n'
        '\n'
        f'target = {json.dumps(product_name)}\n'
        'csv_file = None\n'
        'for f in inputs:\n'
        '    if f.endswith(".csv") or f.endswith(".txt"):\n'
        '        csv_file = f\n'
        '        break\n'
        'if not csv_file:\n'
        '    # try reading first file\n'
        '    csv_file = inputs[0]\n'
        '\n'
        'path = os.path.join("input", csv_file)\n'
        'rows = []\n'
        'with open(path, encoding="utf-8") as f:\n'
        '    reader = csv.DictReader(f)\n'
        '    headers = reader.fieldnames\n'
        '    for row in reader:\n'
        '        rows.append(row)\n'
        '\n'
        'print(f"文件: {csv_file}")\n'
        'print(f"总行数: {len(rows)}")\n'
        'print(f"列名: {headers}")\n'
        '\n'
        '# 查找目标商品\n'
        'target_rows = []\n'
        'target_col = None\n'
        'for col in headers:\n'
        '    if "product" in col.lower() or "name" in col.lower() or "item" in col.lower():\n'
        '        target_col = col\n'
        '        break\n'
        'if target_col:\n'
        '    target_rows = [r for r in rows if target and target.lower() in r.get(target_col, "").lower()]\n'
        '    products = Counter(r.get(target_col, "") for r in rows)\n'
        '    top10 = products.most_common(10)\n'
        '    print(f"\\n商品列: {target_col}")\n'
        '    print(f"命中目标商品行: {len(target_rows)}")\n'
        '    print(f"Top 10 商品: {top10}")\n'
        'else:\n'
        '    print("\\n未识别到商品列")\n'
        '    target_rows = rows[:]\n'
        '\n'
        'if not target_rows:\n'
        '    print("\\n无目标商品数据，跳过分析")\n'
        '    sys.exit(0)\n'
        '\n'
        '# 识别价格列和数量列\n'
        'price_cols = [c for c in headers if "price" in c.lower() or "amount" in c.lower() or "sale" in c.lower()]\n'
        'qty_cols = [c for c in headers if "qty" in c.lower() or "quantity" in c.lower() or "num" in c.lower() or "count" in c.lower()]\n'
        'date_cols = [c for c in headers if "date" in c.lower() or "time" in c.lower()]\n'
        'print(f"\\n候选价格列: {price_cols}")\n'
        'print(f"候选数量列: {qty_cols}")\n'
        'print(f"日期列: {date_cols}")\n'
        '\n'
        '# 尝试各价格列的分布\n'
        'for pc in price_cols:\n'
        '    vals = []\n'
        '    for r in target_rows:\n'
        '        try:\n'
        '            v = float(r.get(pc, "").replace(",", ""))\n'
        '            if v > 0:\n'
        '                vals.append(v)\n'
        '        except:\n'
        '            pass\n'
        '    if vals:\n'
        '        uniq = sorted(set(round(v, 2) for v in vals))\n'
        '        print(f"\\n价格列 [{pc}]: 共 {len(vals)} 个非零值, {len(uniq)} 个唯一价格, "  f"范围 {min(vals):.2f}-{max(vals):.2f}, 中位数 {sorted(vals)[len(vals)//2]:.2f}")\n'
        '\n'
        '# 识别日期列\n'
        'date_col = None\n'
        'for dc in headers:\n'
        '    if "date" in dc.lower():\n'
        '        date_col = dc\n'
        '        break\n'
        'if date_col:\n'
        '    dates = list(set(r.get(date_col, "")[:10] for r in target_rows))\n'
        '    print(f"\\n日期范围: {min(dates)} ~ {max(dates)}, 唯一日期数: {len(dates)}")\n'
        '\n'
        '# 数量统计\n'
        'for qc in qty_cols:\n'
        '    qvals = []\n'
        '    for r in target_rows:\n'
        '        try:\n'
        '            v = float(r.get(qc, "0"))\n'
        '            if v > 0:\n'
        '                qvals.append(v)\n'
        '        except:\n'
        '            pass\n'
        '    if qvals:\n'
        '        print(f"数量列 [{qc}]: 共 {len(qvals)} 个值, 平均 {sum(qvals)/len(qvals):.2f}, 最大 {max(qvals):.0f}, 最小 {min(qvals):.0f}")\n'
        '\n'
        'print("\\n=== 预探索完成 ===")\n'
    )
    ws.write_file("scripts/pre_explore.py", script)
    try:
        r = subprocess.run(
            [sys.executable, str(ws.resolve("scripts/pre_explore.py"))],
            cwd=str(ws.dir), capture_output=True, text=True, timeout=120,
        )
        result = r.stdout
        if r.returncode != 0 and r.stderr:
            result += "\n[stderr]\n" + r.stderr
        return result
    except subprocess.TimeoutExpired:
        return "[预探索超时]"
    except Exception as e:
        return f"[预探索异常: {e}]"


def _build_bootstrap_messages(
    *,
    ws,
    plan_short_text: str,
    list_files_json: str,
    read_document_structure_impl,
    emit_log,
    pre_explore_text: str = "",
) -> list[dict]:
    """Seed the price agent with stable tool-call history before the first model round."""

    def tool_pair(call_id: str, name: str, arguments: dict, content: str) -> list[dict]:
        return [
            {
                "role": "assistant",
                "content": None,
                "reasoning_content": "",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": json.dumps(arguments, ensure_ascii=False),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": call_id,
                "name": name,
                "content": content,
            },
        ]

    messages: list[dict] = []
    if emit_log:
        emit_log("custom_agent", {"level": "info", "message": "✅ bootstrap 注入: list_files"})
    messages.extend(tool_pair("call_bootstrap_list_files", "list_files", {"subdir": ""}, list_files_json))

    if plan_short_text and plan_short_text.strip():
        if emit_log:
            emit_log("custom_agent", {"level": "info", "message": "✅ bootstrap 注入: read_plan_short"})
        messages.extend(tool_pair("call_bootstrap_read_plan_short", "read_plan_short", {}, plan_short_text))

    for idx, rel_path in enumerate(ws.list_inputs()[:3]):
        try:
            if emit_log:
                emit_log("custom_agent", {"level": "info", "message": f"✅ bootstrap 注入: read_document_structure {rel_path}"})
            messages.extend(tool_pair(
                f"call_bootstrap_read_document_structure_{idx}",
                "read_document_structure",
                {"path": f"input/{rel_path}"},
                read_document_structure_impl(ws, f"input/{rel_path}", emit_log=emit_log),
            ))
        except Exception:
            continue

    # Inject old session scripts so Agent doesn't need to read them one by one
    old_scripts_dir = ws.resolve("scripts/old_session_scripts")
    if old_scripts_dir.is_dir():
        for session_dir in sorted(old_scripts_dir.iterdir()):
            if not session_dir.is_dir():
                continue
            for script_file in sorted(session_dir.iterdir()):
                if script_file.suffix != ".py":
                    continue
                try:
                    rel = str(script_file.relative_to(ws.dir))
                    content = script_file.read_text(encoding="utf-8")
                    call_id = f"call_bootstrap_old_script_{script_file.stem}"
                    if emit_log:
                        emit_log("custom_agent", {"level": "info", "message": f"✅ bootstrap 注入: read_file {rel}"})
                    messages.extend(tool_pair(call_id, "read_file", {"path": rel}, content))
                except Exception:
                    continue

    # Inject pre-exploration result
    if pre_explore_text:
        if emit_log:
            emit_log("custom_agent", {"level": "info", "message": "✅ bootstrap 注入: run_python (预探索)"})
        messages.extend(tool_pair(
            "call_bootstrap_pre_explore",
            "run_python",
            {"script_path": "scripts/pre_explore.py"},
            pre_explore_text,
        ))

    return messages
