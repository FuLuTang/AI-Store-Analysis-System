"""Read price recommendation artifacts from a workflow workspace."""

from __future__ import annotations

import json
from pathlib import Path


def read_price_result(run_dir: Path | None) -> tuple[dict | None, str | None]:
    if not run_dir:
        return None, None
    workspace_dir = run_dir / "workspace"
    result = None
    full_result = None
    result_path = workspace_dir / "output" / "price_recommendation.json"
    charts_path = workspace_dir / "output" / "rendered_final_charts.json"
    summary_path = workspace_dir / "summary.md"
    if result_path.exists():
        result = json.loads(result_path.read_text(encoding="utf-8"))
    if result is not None and charts_path.exists():
        try:
            rendered_final_charts = json.loads(charts_path.read_text(encoding="utf-8"))
            if "renderedFinalCharts" not in result:
                result["renderedFinalCharts"] = rendered_final_charts
        except Exception:
            pass
    if summary_path.exists():
        full_result = summary_path.read_text(encoding="utf-8")
    return result, full_result
