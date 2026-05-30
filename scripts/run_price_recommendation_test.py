import os
import sys
import json
import asyncio
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Mock event loop to test
from apps.api.src.main import get_llm_preset
from packages.price_recommendation.workflow import run_price_recommendation_workflow

async def main():
    preset = get_llm_preset("high")
    print("Resolved LLM preset:", {k: (v if k != 'apiKey' else '***') for k, v in preset.items()})

    csv_data = """商品名称,店铺,售价,销量,日期
小葵花金银花露,福州东街口店,35.8,10,2026-05-01
小葵花金银花露,福州东街口店,39.9,5,2026-05-02
小葵花金银花露,鼓楼店,35.8,12,2026-05-01
小葵花金银花露,鼓楼店,39.9,6,2026-05-02
"""

    csv_data2 = """商品名称,店铺,售价,销量,日期
小葵花金银花露,仓山店,32.0,20,2026-05-01
小葵花金银花露,仓山店,38.0,8,2026-05-02
"""

    decoded_files = [
        {"name": "sales_store_a.csv", "bytes": csv_data.encode("utf-8")},
        {"name": "sales_store_b.csv", "bytes": csv_data2.encode("utf-8")},
    ]

    workspace_dir = PROJECT_ROOT / "storage" / "test_run_price_rec"
    
    # Clean workspace first
    import shutil
    if workspace_dir.exists():
        shutil.rmtree(workspace_dir)
    workspace_dir.mkdir(parents=True, exist_ok=True)

    def emit_log(node_id, payload):
        print(f"[{node_id}] {payload}")

    print("Starting price recommendation workflow...")
    result, summary = await asyncio.to_thread(
        run_price_recommendation_workflow,
        decoded_files=decoded_files,
        product_name="小葵花金银花露",
        candidate_count=2,
        workspace_dir=workspace_dir,
        llm_preset=preset,
        emit_log=emit_log,
    )

    print("\nWorkflow completed successfully!")
    print("Result json:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\nSummary markdown:")
    print(summary)

if __name__ == "__main__":
    asyncio.run(main())
