"""Prompt builder placeholder for the future price recommendation Agent Runner."""

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def build_system_content() -> str:
    return (
        "你是个销售数据整理Agent，你将要把数据进行入库和归一化处理。"
        "在归一化时，必须先统一时间颗粒度，优先选择能让价格-销量关系更具统计性的粒度；如果点位过于稀疏，必须改用更大的时间颗粒度。"
        "在 `scripts/old_session_scripts/` 目录下（如果存在）存有该用户最近几次运行生成的旧 Python 脚本（按 run_id 文件夹分类）。如果你发现有可复用的清洗/归一化脚本，你可以通过 `run_python` 直接执行它们（系统会自动将它们复制到你的根 `scripts/` 目录中）。注意：你没有直接修改或写入 `old_session_scripts/` 目录的权限。\n"
        "当计划的全部步骤完成，或者遇到无法继续的严重错误时，必须调用 finish_task 结束/终止当前任务。"
    )

def build_user_content(product_name: str, candidate_count: int = 2) -> str:
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M")
    return (
        f"当前时间：{now}（北京时间）\n\n"
        f"目标商品：{product_name}\n"
        "所有步骤全部完成后（或者遇到不可恢复的错误），调用 finish_task 结束任务。"
    )
