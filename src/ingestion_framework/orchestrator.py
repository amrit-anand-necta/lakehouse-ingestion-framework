"""Orchestrator — loops all active configs and runs each pipeline.

Failure isolation: if config_id=2 fails, config_id=3 still runs.
After all tables are processed, if ANY failed, the orchestrator raises
an exception — this causes the overall job to be marked as failed for
alerting, while still having processed every table it could.

This mirrors exactly how your kotak 00_orchestrator.py works — the same
"continue on failure, raise at the end" pattern.
"""

from __future__ import annotations

import uuid

from pyspark.sql import SparkSession

from ingestion_framework.config import get_all_active_configs
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.pipeline import run_pipeline

logger = get_logger(__name__)


def run_all(spark: SparkSession) -> None:
    """Run pipelines for all active configs.

    batch_id is generated once here and shared across all table runs —
    lets you query "everything that ran in last night's batch" with a
    single filter on the audit table.
    """
    batch_id = str(uuid.uuid4())
    logger.info("Batch START | batch_id=%s", batch_id[:8])

    configs = get_all_active_configs(spark)
    if not configs:
        logger.warning("No active configs found — nothing to run")
        return

    results = []
    for config in configs:
        result = run_pipeline(config, spark, batch_id)
        results.append(result)

    # Summary
    succeeded = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]

    logger.info(
        "Batch END | batch_id=%s | total=%d | succeeded=%d | failed=%d",
        batch_id[:8], len(results), len(succeeded), len(failed),
    )

    if failed:
        failed_ids = [r["config_id"] for r in failed]
        raise RuntimeError(
            f"Batch completed with failures. Failed config_ids: {failed_ids}"
        )
