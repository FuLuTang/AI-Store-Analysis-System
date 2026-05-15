"""底层纯函数：数据画像"""
import json
from ...workspace import Workspace


def profile_table_impl(ws: Workspace, parquet_path: str) -> str:
    import duckdb
    con = duckdb.connect(ws.duckdb_path)
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
