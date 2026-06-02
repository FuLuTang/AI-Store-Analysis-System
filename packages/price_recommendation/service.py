"""Service entrypoints for the price recommendation feature."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from .precheck import run_precheck
from .result_reader import read_price_result
from .workflow import run_price_recommendation_workflow

logger = logging.getLogger("price_recommendation")

LogCallback = Callable[[str, dict], None]
AbortCallback = Callable[[], None]


def run_price_precheck(decoded_files: list[dict], product_name: str) -> dict:
    logger.info("Running price precheck, product=%s, files=%d", product_name, len(decoded_files))
    result = run_precheck(decoded_files, product_name)
    logger.info("Precheck result: valid=%s, issues=%d, warnings=%d",
                result.get("valid"), len(result.get("issues", [])), len(result.get("warnings", [])))
    return result


def run_price_workflow(
    *,
    decoded_files: list[dict],
    product_name: str,
    candidate_count: int,
    workspace_dir: Path,
    llm_preset: dict,
    emit_log: LogCallback,
    check_aborted: AbortCallback | None = None,
) -> tuple[dict, str]:
    logger.info("Starting price workflow, product=%s, candidate_count=%d, workspace=%s",
                product_name, candidate_count, workspace_dir)
    result, summary = run_price_recommendation_workflow(
        decoded_files=decoded_files,
        product_name=product_name,
        candidate_count=candidate_count,
        workspace_dir=workspace_dir,
        llm_preset=llm_preset,
        emit_log=emit_log,
        check_aborted=check_aborted,
    )
    logger.info("Price workflow completed, recommendations=%d",
                len(result.get("recommendations", [])))
    return result, summary


def read_price_service_result(run_dir: Path | None) -> tuple[dict | None, str | None]:
    logger.info("Reading price service result from %s", run_dir)
    result, summary = read_price_result(run_dir)
    if result is None:
        logger.warning("Price result not found at %s", run_dir)
    return result, summary
