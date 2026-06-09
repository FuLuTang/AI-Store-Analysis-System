"""
系统诊断功能 (ai_analyse/launch_diagnosis)

功能描述：
对门店经营数据文件进行 AI 智能分析与诊断，生成诊断报告。它会自动完成数据清洗、指标计算、异常诊断并产出最终的诊断摘要。

参数说明：
- file_names (list[str]): 必须。需要诊断的文件名称列表。文件必须已经上传到 Chatbot 系统（通常路径为 "chatbot/files/[stored_name]"，可以直接传入相对路径）。
- reasoning_effort (str): 可选。AI 的推理力度。可选值为 "low", "medium", "high"，默认值为 "medium"。
- analysis_params (str): 可选。额外的分析指令或参数约束。

示例参数：
{
  "file_names": ["chatbot/files/sales_data.xlsx"],
  "reasoning_effort": "high"
}
"""

import asyncio
import uuid
import json
from pathlib import Path
from packages.agents.registry import create_pipeline
from packages.core.input_adapter import parse_uploaded_files, adapt_to_dataset_bundle
from packages.agents.core.models import RawFile

def run(ws, params: dict, llm_preset: dict) -> str:
    # 1. 解析参数
    file_names = params.get("file_names", [])
    if not file_names:
        return "错误：未指定需要分析的文件名列表（file_names）。"
        
    reasoning_effort = params.get("reasoning_effort", "medium")
    analysis_params = params.get("analysis_params", "")

    # 复用/覆盖 llm_preset 配置
    active_preset = llm_preset.copy()
    active_preset["reasoningEffort"] = reasoning_effort
    if "call" in active_preset:
        active_preset["call"] = active_preset["call"].copy()
        active_preset["call"]["reasoningEffort"] = reasoning_effort

    decoded_files = []
    for f in file_names:
        try:
            # 优先通过 workspace.resolve_read 找文件
            file_path = ws.resolve_read(f)
        except Exception:
            file_path = ws.write_root / f
        
        if not file_path.exists():
            return f"错误：未找到文件 '{f}'。请确认文件已上传且路径正确。"
        
        decoded_files.append({
            "name": file_path.name,
            "bytes": file_path.read_bytes()
        })

    # 2. 转换为 DatasetBundle
    try:
        raw_bundle = parse_uploaded_files(decoded_files)
        bundle = adapt_to_dataset_bundle(raw_bundle)
        for df in decoded_files:
            bundle.raw_files.append(RawFile(name=df["name"], data=df["bytes"]))
    except Exception as e:
        return f"错误：解析文件失败，详细信息: {str(e)}"

    # 3. 为本次运行创建独立的隔离工作区
    run_id = uuid.uuid4().hex[:12]
    diagnosis_ws_dir = ws.write_root / "runs" / "diagnosis" / run_id
    diagnosis_ws_dir.mkdir(parents=True, exist_ok=True)

    # 4. 创建诊断 Pipeline
    pipe = create_pipeline(
        "custom",
        llm_preset=active_preset,
        check_aborted=None,
        workspace_dir=diagnosis_ws_dir,
        analysis_params=analysis_params
    )
    
    # 简单输出日志到 stdout
    pipe.set_event_callbacks(
        on_log=lambda nid, msg: print(f"[diagnosis-{run_id}] [{nid}] {msg}"),
        on_status=lambda nid, st: print(f"[diagnosis-{run_id}] [{nid}] 状态 -> {st}")
    )

    # 5. 在新事件循环中运行 Pipeline (因为当前在 asyncio.to_thread 线程中，无运行中的 event loop)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        print(f"开始执行系统诊断任务，run_id={run_id} ...")
        loop.run_until_complete(pipe.run(bundle))
    except Exception as e:
        return f"错误：诊断任务执行过程中发生异常: {str(e)}"
    finally:
        loop.close()

    # 6. 读取诊断摘要报告返回
    summary_path = diagnosis_ws_dir / "output" / "summary.md"
    if summary_path.exists():
        report_text = summary_path.read_text(encoding="utf-8")
        return f"系统诊断完成。报告已生成，以下是诊断报告内容：\n\n{report_text}"
    else:
        return f"系统诊断运行完毕，但未能成功生成最终报告 summary.md。诊断 ID：{run_id}。"
