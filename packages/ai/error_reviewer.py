"""
error_reviewer.py — 错误评审模块 (v2: 场景感知 + 格式化输入)
"""
import json
from datetime import datetime
from .ai_caller import shared_stream_fetch


def _build_review_prompt(scene: dict = None) -> str:
    return """你是一位极其严谨的数据审计专家。任务：复核初级报告中是否存在明显错误。

重点检查：
1. 数据引用错误：报告引用的数值是否与【证据包】中的数据一致？
2. 周期换算错误：环比/月变化结论是否考虑了时间口径（如未到月底）？
3. 指标交叉矛盾：不同指标的结论是否互相矛盾（如营收涨但毛利跌未解释）？
4. 缺数据误判：标记为 uncountable 的指标是否在报告中被引用或编造？
5. 建议可行性：行动方案是否符合当前数据能力和行业常识？
6. 干扰信息过滤：排名、排序、ID等元数据字段不应作为经营判断依据

输出要求：
- 纯文本评审意见
- 没问题 → "审核通过，未发现明显逻辑或数据错误。"
- 有问题 → 逐条列出 "发现异常：1. ... 2. ..."，每条注明引用哪条证据
- 直接输出结论，无客套话"""


def _format_evidence_for_review(evidence_bundle: dict) -> str:
    items = evidence_bundle.get("items", [])
    if not items:
        return "暂无证据数据。"

    lines = ["【证据包（复核基准）】"]
    lines.append(f"共 {len(items)} 条证据:")
    for item in items:
        metric_id = item.get("metric_id", "?")
        title = item.get("title", "?")
        status = item.get("status", "?")
        val = item.get("value")
        if isinstance(val, (dict, list)):
            val_str = json.dumps(val, ensure_ascii=False)[:120]
        else:
            val_str = str(val)[:80] if val is not None else "-"
        lines.append(f"  [{status}] {title} | value={val_str}")
    return "\n".join(lines)


async def review_error(settings: dict, report: str, cleaned_data_texts: list = None) -> dict | str:
    """错误评审 (兼容旧管线)"""
    if not settings or not settings.get("apiKey"):
        return "审核通过：未配置 API Key，已跳过。"

    now = datetime.now()
    context = f"【分析环境】\n- 日期：{now.strftime('%Y年%m月%d日')} {now.strftime('%H:%M')}\n"
    raw_str = "\n\n---\n\n".join(cleaned_data_texts) if cleaned_data_texts else "暂无底层数据"
    user_content = context + "\n\n【初级分析报告】\n" + (report or "") + "\n\n【底层数据】\n" + raw_str

    reasoning_effort = "medium"
    if isinstance(settings, dict):
        raw = settings.get("reasoningEffort") or settings.get("reasoning_effort") or ""
        r = str(raw).strip().lower()
        if r in {"low", "medium", "high"}:
            reasoning_effort = r

    try:
        return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
            "model": settings["model"],
            "messages": [
                {"role": "system", "content": _build_review_prompt()},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2, "reasoning_effort": reasoning_effort, "max_tokens": 8192,
        })
    except Exception as e:
        return f"错误审核失败。原因：{str(e)}"


async def review_error_new(settings: dict, scene: dict, report: str, evidence: dict) -> dict | str:
    """错误评审 (新管线 — 格式化输入)"""
    if not settings or not settings.get("apiKey"):
        return "审核通过：未配置 API Key，已跳过。"

    reasoning_effort = "medium"
    if isinstance(settings, dict):
        raw = settings.get("reasoningEffort") or settings.get("reasoning_effort") or ""
        r = str(raw).strip().lower()
        if r in {"low", "medium", "high"}:
            reasoning_effort = r

    now = datetime.now()
    parts = [
        f"【分析环境】\n- 行业：{(scene or {}).get('industry', 'unknown')}\n- 日期：{now.strftime('%Y年%m月%d日 %H:%M')}",
        "",
        f"【初级分析报告】\n{report}",
        "",
        _format_evidence_for_review(evidence),
    ]
    user_content = "\n".join(parts)

    try:
        return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
            "model": settings["model"],
            "messages": [
                {"role": "system", "content": _build_review_prompt(scene)},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2, "reasoning_effort": reasoning_effort, "max_tokens": 8192,
        })
    except Exception as e:
        return f"错误审核失败。原因：{str(e)}"
