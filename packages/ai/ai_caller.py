"""
ai_caller.py — AI 调用模块 (从 ai-caller.js 完整迁移)
包含: callAI (初级报告), callDetailedAI (详细报告), callSimplifiedAI (精简报告)
"""
import json
import asyncio
from datetime import datetime

SYSTEM_PROMPT = """
你是一位资深连锁药店经营分析顾问。你将收到一家门店的多维经营数据，数据已清洗为紧凑格式。

数据类型：
- business_overview：营业概览，含营收、来客数、客单价、毛利、会员、电商、业绩来源占比及环比趋势
- store_hot_products：门店热销商品，按今/昨/周/月分组
- hot_top500_stock_status：全城热销TOP500库存对照
- **Algorithm Analysis Results**: 算法引擎预先计算的诊断指标和异常清单，请优先参考其中的结论。

你的任务：
基于数据生成一份【经营诊断报告】，聚焦颗粒度为年、月、周。请将现状诊断报告与优化行动方案（如果有）写在一起。

# 第一部分：现状诊断报告
1. 核心经营判断 （现在是涨是跌还是稳定还是波动，可能原因是什么，数据是否正常，带emojis如📈📉）
2. 热销商品变化趋势分析
3. 缺货/缺种损失评估
4. 风险预警（可选）

# 第二部分：优化行动方案（可选部分，不一定都有）
1. 紧急补货
2. 品种引进
3. 毛利/客单提升
4. 促活
5. 根据你的经验，能想到的其他可能有效的方案

ideas:
1. 考虑到店的位置，气温，时间天气，社会环境因素，近期事件。开拓思维
2. 可结合当前季节、气温（春/夏季交替）及社会因素进行发散性诊断
3. 因为没有同比数据，所以环比数据要注意折算！

输出要求：
- Markdown 格式，大小标题，列点，格式化，必要时用表格或Mermaid美化输出
- 两大段：用 # 现状诊断报告 ... \\n --- \\n # 优化行动方案 ... 来分隔
- 全文控制在约1000字
- 不要使用过长段 bullet
- 每个 bullet 不超过 45 字
- 总结只保留 4 个核心问题
- 商品最多提 6 个
- 不要把"建议："写成单独一行
"""

DETAILED_SYSTEM_PROMPT = """
你是一位资深数据分析师与零售专家。你将收到一份经过初步处理的【门店分析融合报告】，包含：
1. 初级AI诊断报告
2. 明显错误评审意见
3. 算法引擎检测出的异常日志

你的任务：
基于这些融合信息，深度重写并输出一份【更详细、结构更严谨的最终经营诊断报告】。

注意：
- 不需要复述那些能通过原始json/ERP系统里能直接看出来的浅层信息
- 初级报告中可能存在谬误，请务必结合评审意见进行核对
- "指标"是用来补充初级报告中未发现的问题的
- "指标"中的结果很可能和初级报告有冲突，请自行判断并融合

你需要：
- 纠正初级报告中被"错误评审"指出的逻辑或计算谬误。
- 结合"异常检测日志"，挖掘更深层次的业务根因。
- 提供更加具体、可落地的优化行动方案。
- 保持专业的商业分析语调。
- 格式化输出，采用合适的标题、列表和加粗，让重点一目了然。
- 结尾不需要"如果你愿意，我可以帮你..."等字样
- 不要使用"不是，而是"句式
"""

SIMPLIFIED_SYSTEM_PROMPT = """
你是一位资深连锁药店经营分析顾问，你的任务是为管理层（老板）提供一份"一眼定真问题"的【精简诊断报告】。
你将收到一份经过深度分析的详细报告。请提取最核心的信息，并严格按照以下 JSON 格式输出，不要包含任何其他字符或 Markdown 格式（不要包含 ```json）：

{
  "health_status": "这里填1-2个词的整体状态",
  "overview_text": "用一句大白话总结门店当前的整体经营状况。",
  "cards": [
    {
      "title": "问题标题",
      "explanation": "大白话说怎么回事。",
      "suggestion": "咋办。",
      "evidence": "相关数据证据。优先使用 Markdown 迷你表格展示。",
      "color": "red/yellow/green/blue/pink"
    }
  ]
}

要求：
1. cards 最多只能有 7 个。
2. 语言必须是大白话。
3. 如果发现数据口径不一致，请使用 pink 颜色。
4. 禁止"不是，而是"句式
"""


async def shared_stream_fetch(base_url: str, api_key: str, payload: dict, abort_event=None) -> dict:
    """通用流式请求助手，用于绕过超时限制"""
    import httpx

    url = base_url.rstrip("/") + "/chat/completions"
    payload["stream"] = True

    full_text = ""
    usage = None

    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
        async with client.stream(
            "POST", url,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json=payload,
        ) as response:
            if response.status_code != 200:
                err = await response.aread()
                raise Exception(f"AI API 调用失败 ({response.status_code}): {err.decode()[:300]}")

            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                lines = buffer.split("\n")
                buffer = lines.pop()
                for line in lines:
                    line = line.strip()
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str == "[DONE]":
                            continue
                        try:
                            data_obj = json.loads(data_str)
                            delta = data_obj.get("choices", [{}])[0].get("delta", {})
                            if delta.get("content"):
                                full_text += delta["content"]
                            if data_obj.get("usage"):
                                usage = data_obj["usage"]
                        except json.JSONDecodeError:
                            pass

    return {"choices": [{"message": {"content": full_text}}], "usage": usage}


def _build_context_header() -> str:
    now = datetime.now()
    return f"【当前分析环境】\n- 城市：福州\n- 日期：{now.strftime('%Y年%m月%d日')}\n- 时间：{now.strftime('%H:%M')}\n"


def _resolve_reasoning_effort(settings: dict) -> str:
    raw = ""
    if isinstance(settings, dict):
        raw = settings.get("reasoningEffort") or settings.get("reasoning_effort") or ""
    value = str(raw).strip().lower()
    return value if value in {"low", "medium", "high"} else "medium"


async def call_ai(settings: dict, cleaned_data_texts: list, algo_data: dict = None) -> dict:
    """AI-1: 初级报告"""
    context = _build_context_header()
    reasoning_effort = _resolve_reasoning_effort(settings)

    algo_text = "【算法引擎预诊结果】\n暂无算法诊断结果。"
    if algo_data and algo_data.get("anomalies"):
        a = algo_data["anomalies"]
        algo_text = f"【算法引擎预诊结果】\n> {a.get('summary', '')}\n\n"
        for alert in a.get("alerts", []):
            icon = "🔴" if alert.get("severity") == "warning" else "🟡"
            algo_text += f"{icon} **{alert['metric']}** ({alert['severity']}): {alert['detail']}\n"

    user_content = context + "\n" + algo_text + "\n\n【底层原始数据】\n" + "\n\n---\n\n".join(cleaned_data_texts)

    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 16384,
    })


async def call_detailed_ai(settings: dict, fused_report_text: str) -> dict:
    """AI-3: 详细报告"""
    user_content = _build_context_header() + "\n" + fused_report_text
    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": DETAILED_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "reasoning_effort": reasoning_effort, "temperature": 0.4, "max_tokens": 16384,
    })


async def call_simplified_ai(settings: dict, detailed_report_text: str) -> dict:
    """AI-4: 老板视图 (精简报告)"""
    user_content = _build_context_header() + "\n\n【详细报告内容】\n" + detailed_report_text
    reasoning_effort = _resolve_reasoning_effort(settings)
    return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
        "model": settings["model"],
        "messages": [
            {"role": "system", "content": SIMPLIFIED_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "reasoning_effort": reasoning_effort, "temperature": 0.3, "max_tokens": 8192,
    })
