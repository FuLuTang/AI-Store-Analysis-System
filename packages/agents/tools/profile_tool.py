"""数据画像（委托到 impl）"""
from .impl.profile_impl import profile_table_impl
from ..workspace import Workspace


def profile_table(ws: Workspace, parquet_path: str) -> str:
    return profile_table_impl(ws, parquet_path)
