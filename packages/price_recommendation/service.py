"""Service entrypoints for the price recommendation feature."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from .precheck import run_precheck
from .result_reader import read_price_result
from .workflow import run_price_recommendation_workflow


LogCallback = Callable[[str, dict], None]
AbortCallback = Callable[[], None]


def run_price_precheck(decoded_files: list[dict], product_name: str) -> dict:
    return run_precheck(decoded_files, product_name)


def run_price_workflow(
    *,
    decoded_files: list[dict],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path,
    emit_log: LogCallback,
    check_aborted: AbortCallback | None = None,
) -> tuple[dict, str]:
    return run_price_recommendation_workflow(
        decoded_files=decoded_files,
        product_name=product_name,
        candidate_count=candidate_count,
        workspace_dir=workspace_dir,
        emit_log=emit_log,
        check_aborted=check_aborted,
    )


def read_price_service_result(run_dir: Path | None) -> tuple[dict | None, str | None]:
    return read_price_result(run_dir)
