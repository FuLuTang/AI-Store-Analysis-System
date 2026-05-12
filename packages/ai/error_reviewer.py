"""
error_reviewer.py — 错误评审模块 (从 error-reviewer.js 完整迁移)
"""
import json
from datetime import datetime
from .ai_caller import shared_stream_fetch

REVIEW_SYSTEM_PROMPT = """
你是一位极其严谨的零售数据审计专家。你的任务是"检查初级报告中是否有明显数据异常"，进行二次复核。
你需要重点关注以下几类常见分析错误，并根据提供给你的【底层原始数据】进行核对：
1. **周期换算错误**：比如年/月下滑 20% 是否仅仅是因为当前还没到年/月底？
2. **无意义商品的干扰**：矿泉水、塑料袋等低毛利/高频凑单商品是否影响了判断？
3. **数据自相矛盾**：结论是否与提供的数据明显冲突？
4. **因缺少数据导致的无法分析或分析不到位**
5. **推荐行动的可执行性**：考虑到当前店定位和环境常识

**输出要求：**
- 纯文本形式的【评审意见】。
- 如果没问题，只需回答："错误审核通过，暂未发现明显逻辑或计算谬误。"
- 如果发现了问题，请逐条列出："发现异常：1. ... 2. ..."
- 请直接输出结论，无需客套话。
"""


async def review_error(settings: dict, report: str, cleaned_data_texts: list = None) -> dict | str:
    """错误评审 AI 调用"""
    if not settings or not settings.get("apiKey"):
        return "初级分析报告 - 错误审核：未配置 API Key，已跳过真实审核，采用模拟通过。"

    now = datetime.now()
    context = f"【当前分析环境】\n- 城市：福州\n- 日期：{now.strftime('%Y年%m月%d日')}\n- 时间：{now.strftime('%H:%M')}\n"

    raw_data_str = "\n\n---\n\n".join(cleaned_data_texts) if cleaned_data_texts else "暂无底层数据"
    user_content = context + "\n\n【初级分析报告】\n" + (report or "") + "\n\n【底层原始数据】\n" + raw_data_str
    raw_effort = settings.get("reasoningEffort") or settings.get("reasoning_effort") or ""
    reasoning_effort = str(raw_effort).strip().lower()
    if reasoning_effort not in {"low", "medium", "high"}:
        reasoning_effort = "medium"

    try:
        return await shared_stream_fetch(settings["baseUrl"], settings["apiKey"], {
            "model": settings["model"],
            "messages": [
                {"role": "system", "content": REVIEW_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2, "reasoning_effort": reasoning_effort, "max_tokens": 8192,
        })
    except Exception as e:
        return f"错误审核失败。原因：{str(e)}"
