"""Prompt builder placeholder for the future price recommendation Agent Runner."""

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def build_system_content() -> str:
    return (
        "你是一个最优价格推荐 Agent。所有价格建议必须基于工具读取到的真实数据，"
        "不得编造字段、销量、价格或日期。最终必须写入 output/price_recommendation.json。\n"
        "当计划的全部步骤完成，或者遇到无法继续的严重错误时，必须调用 finish_task 结束/终止当前任务。"
    )

def build_user_content(product_name: str, candidate_count: int = 2) -> str:
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M")
    return (
        f"当前时间：{now}（北京时间）\n\n"
        f"目标商品：{product_name}\n"
        f"需要返回推荐价格数量：{candidate_count}\n\n"
        "按 plan 中步骤推进。先侦察文件结构，再定位商品和字段，最后输出 JSON。\n"
        "所有步骤全部完成后（或者遇到不可恢复的错误），调用 finish_task 结束任务。"
    )
