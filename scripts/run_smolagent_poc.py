"""
Smolagents CodeAgent PoC — 入口脚本

用法：
    python scripts/run_smolagent_poc.py
    python scripts/run_smolagent_poc.py --data data/examples/概览-日.json --max-iter 10

依赖：
    pip install smolagents duckdb pandas httpx
"""
import argparse
import json
import os
import sys

ROOT_DIR = Path(__file__).parent.parent
sys.path.append(str(ROOT_DIR))


def flatten_dataset(raw: dict) -> list[dict]:
    """递归展平嵌套 dict/list 为 flat rows（由 Agent 自己生成，这里作为 fallback）"""
    rows = []

    def _walk(obj, prefix=""):
        if isinstance(obj, dict):
            leaf = True
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    leaf = False
                    _walk(v, f"{prefix}{k}.")
                else:
                    if leaf:
                        rows.append({prefix.strip("."): obj})
        elif isinstance(obj, list):
            for item in obj:
                _walk(item, prefix)

    _walk(raw)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Smolagents CodeAgent PoC")
    parser.add_argument("--data", default="data/examples/概览-日.json", help="输入数据路径")
    parser.add_argument("--max-iter", type=int, default=10, help="Agent 最大迭代步数")
    parser.add_argument("--no-agent", action="store_true", help="跳过 Agent，只用 fallback 展平测试")
    args = parser.parse_args()

    data_path = ROOT_DIR / args.data

    if not data_path.exists():
        print(f"[错误] 文件不存在: {data_path}")
        sys.exit(1)

    print(f"=== Smolagents CodeAgent PoC ===")
    print(f"输入: {data_path}")
    print(f"最大迭代: {args.max_iter}")

    with open(data_path) as f:
        raw = json.load(f)

    if args.no_agent:
        print("\n--- fallback 展平 ---")
        rows = flatten_dataset(raw)
        print(f"展平后: {len(rows)} 行, {len(rows[0]) if rows else 0} 字段")
        print(json.dumps(rows[:3], ensure_ascii=False, indent=2))
        return

    try:
        from smolagents import CodeAgent, HfApiModel, PythonTool, tool
    except ImportError:
        print("[错误] 请先安装 smolagents: pip install smolagents")
        print("或者使用 --no-agent 跳过 Agent 只用展平测试")
        sys.exit(1)

    @tool
    def duckdb_query(sql: str) -> str:
        """在内存 DuckDB 实例上执行 SQL 返回 JSON

        Args:
            sql: 要执行的 SQL 语句
        Returns:
            JSON 字符串格式的查询结果
        """
        import duckdb
        import pandas as pd

        con = duckdb.connect(":memory:")
        try:
            result = con.execute(sql).fetchdf()
            return json.dumps(json.loads(result.to_json(orient="records")), ensure_ascii=False)
        finally:
            con.close()

    agent = CodeAgent(
        tools=[PythonTool(), duckdb_query],
        model=HfApiModel("deepseek-chat"),
        max_iterations=args.max_iter,
    )

    prompt = f"""
读取 {data_path}，
将其展平为二维表并注册到 DuckDB，
然后计算：总营收 (retail_amount sum)、渠道占比 (channel share)、环比变化 (period change)。
输出 JSON 格式结果。
    """

    print("\n--- 启动 CodeAgent ---")
    result = agent.run(prompt)
    print("\n--- 结果 ---")
    print(result)


if __name__ == "__main__":
    from pathlib import Path
    main()
