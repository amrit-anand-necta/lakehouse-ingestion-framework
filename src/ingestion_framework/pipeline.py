"""Pipeline — the per-table ingestion lifecycle.

This is the heart of the framework. One call to run_pipeline() handles
the complete lifecycle for one table:

  read → validate → write → audit (start + end) → watermark update

The pipeline knows nothing about source types or write strategies — those
decisions live in the readers and writers. The pipeline just calls them
in the right order and handles failures gracefully.

This is the Template Method pattern:
  - the STEPS are fixed (read, validate, write, audit)
  - the IMPLEMENTATION of each step is pluggable (csv vs jdbc, full vs merge)
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from ingestion_framework.audit import generate_run_id, write_audit
from ingestion_framework.config import IngestionConfig, update_watermark
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.validation import validate
from ingestion_framework.writers.delta_writer import write_table

logger = get_logger(__name__)


def _get_reader(config: IngestionConfig):
    """Return the correct reader function based on source_type.

    This is the dispatcher pattern — one place maps source_type strings to
    reader functions. Adding a new source type = import it here + add one case.
    The pipeline and orchestrator never need to change.
    """
    if config.source_type == "csv":
        from ingestion_framework.readers.csv_reader import read_csv
        return read_csv
    else:
        raise ValueError(
            f"config_id={config.config_id}: unsupported source_type '{config.source_type}'. "
            f"Supported: csv"
        )


def run_pipeline(
    config: IngestionConfig,
    spark: SparkSession,
    batch_id: str,
) -> dict:
    """Run the full ingestion lifecycle for one table.

    Returns a summary dict with status and counts — the orchestrator uses
    this to decide whether to raise an error at the end of the batch.

    Args:
        config:   the pipeline's config row
        spark:    active SparkSession
        batch_id: shared ID for this entire job run (same for all tables)
    """
    run_id = generate_run_id()
    start_time = datetime.utcnow()

    logger.info(
        "Pipeline START | config_id=%d | source=%s | target=%s | load=%s | run_id=%s",
        config.config_id, config.source_table, config.target_fqn,
        config.load_type, run_id[:8],
    )

    # --- Write START audit record ---
    # This happens before any work so a crash mid-run leaves a trace.
    write_audit(
        spark=spark,
        run_id=run_id,
        batch_id=batch_id,
        config_id=config.config_id,
        source_system=config.source_system,
        source_table=config.source_table or "",
        target_table=config.target_fqn,
        status="started",
        load_type=config.load_type,
        start_time=start_time,
    )

    try:
        # --- Step 1: Read ---
        reader = _get_reader(config)
        df = reader(config, spark)

        # --- Step 2: Validate ---
        result = validate(df, config, spark)

        if result.status == "failed":
            raise ValueError(
                f"Validation failed for config_id={config.config_id}: "
                f"0 rows passed validation"
            )

        # --- Step 3: Write ---
        rows_written = write_table(result.df, config, spark)

        # --- Step 4: Update watermark (incremental loads only) ---
        if config.load_type == "incremental" and config.has_watermark:
            # Find the MAX value of the watermark column in what we just wrote.
            # This becomes the new "last loaded" value for the next run.
            from pyspark.sql import functions as F
            new_watermark = (
                result.df
                .agg(F.max(config.incremental_key_1).alias("max_val"))
                .collect()[0]["max_val"]
            )
            if new_watermark:
                update_watermark(spark, config.config_id, str(new_watermark))

        end_time = datetime.utcnow()

        # --- Write END audit record (success) ---
        write_audit(
            spark=spark,
            run_id=run_id,
            batch_id=batch_id,
            config_id=config.config_id,
            source_system=config.source_system,
            source_table=config.source_table or "",
            target_table=config.target_fqn,
            status="success",
            load_type=config.load_type,
            source_count=result.source_count,
            records_inserted=rows_written,
            rejected_records=result.rejected_count,
            validation_status=result.status,
            start_time=start_time,
            end_time=end_time,
        )

        duration = (end_time - start_time).total_seconds()
        logger.info(
            "Pipeline SUCCESS | config_id=%d | inserted=%d | rejected=%d | %.1fs",
            config.config_id, rows_written, result.rejected_count, duration,
        )

        return {
            "config_id": config.config_id,
            "status": "success",
            "rows_written": rows_written,
            "rejected": result.rejected_count,
        }

    except Exception as e:
        end_time = datetime.utcnow()
        error_msg = str(e)[:500]  # truncate very long stack traces

        logger.error(
            "Pipeline FAILED | config_id=%d | error=%s",
            config.config_id, error_msg,
        )

        # --- Write END audit record (failure) ---
        # Even on failure we write an audit record — this is what ops queries
        # to find failed runs without digging through logs.
        write_audit(
            spark=spark,
            run_id=run_id,
            batch_id=batch_id,
            config_id=config.config_id,
            source_system=config.source_system,
            source_table=config.source_table or "",
            target_table=config.target_fqn,
            status="failed",
            load_type=config.load_type,
            error_message=error_msg,
            validation_status="failed",
            start_time=start_time,
            end_time=end_time,
        )

        return {
            "config_id": config.config_id,
            "status": "failed",
            "error": error_msg,
        }
