"""上下文工具：读取指标文档、字段定义、行业规则。"""


async def read_context_tool(topic: str) -> str:
    """按主题读取上下文文档（指标计算文档、行业包等）。
    
    topic: "metrics" | "fields" | "pharmacy" | "restaurant" | "hr" | "common"
    """
    ...
