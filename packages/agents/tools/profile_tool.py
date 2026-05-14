"""画像工具：查看表结构、列名、样本值、空值率。"""

from packages.agents.models import ColumnMeta, TableMeta
from packages.agents.workspace import Workspace


async def profile_table(ws: Workspace, table_name: str) -> TableMeta:
    """返回指定表的完整 metadata（列名、类型、样本、空值率）。"""
    ...


async def profile_all_tables(ws: Workspace) -> list[TableMeta]:
    """返回 workspace 中所有表的 metadata。"""
    ...
