"""画像工具：查看表结构、列名、样本值、空值率。"""

import json

from ..workspace import Workspace


def profile_table(parquet_path: str) -> str:
    """读取 parquet 文件，返回字段画像（列名、类型、样本）"""
    import duckdb
    con = duckdb.connect(":memory:")
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 100").fetchdf()
        profiles = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            samples = df[col].dropna().head(5).tolist()
            null_rate = df[col].isna().mean()
            profiles[col] = {"dtype": dtype, "samples": samples, "null_rate": round(null_rate, 3)}
        return json.dumps(profiles, ensure_ascii=False, default=str)
    finally:
        con.close()


async def profile_workspace(ws: Workspace) -> list:
    """返回 workspace 中所有表的 metadata（待实现）。"""
    ...
