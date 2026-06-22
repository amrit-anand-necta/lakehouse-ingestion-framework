"""Audit table writer.

Every pipeline run writes two records to the audit table:
  1. START record — written before any work begins
  2. END record   — written after success or failure, with full metrics

Why two records?
If the pipeline crashes mid-run, the START record is already in the audit table.
Operations can query: "show me all runs that have a START but no END" — those
are the hung or crashed pipelines. Without the START record you'd have no trace
that the run ever attempted.

The audit table is the first thing you open when something goes wrong at 2am.
Good audit records mean the answer is in the table. Bad ones mean you're
digging through logs hoping something was printed.
"""

from __future__ import annotations

import uuid
from datetime import datetime, date
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType, DoubleType,
    TimestampType, DateType,
)

from ingestion_framework.config import AUDIT_TABLE_PATH
from ingestion_framework.logging_setup import get_logger

# Explicit schema — required when any field may be None at creation time.
# Spark cannot infer a type from None, so we tell it the types explicitly.
AUDIT_SCHEMA = StructType([
    StructField("audit_id",          LongType(),      True),
    StructField("run_id",            StringType(),    False),
    StructField("batch_id",          StringType(),    False),
    StructField("config_id",         IntegerType(),   False),
    StructField("source_system",     StringType(),    True),
    StructField("source_table",      StringType(),    True),
    StructField("target_table",      StringType(),    True),
    StructField("status",            StringType(),    False),
    StructField("load_type",         StringType(),    True),
    StructField("source_count",      LongType(),      True),
    StructField("records_inserted",  LongType(),      True),
    StructField("rejected_records",  LongType(),      True),
    StructField("validation_status", StringType(),    True),
    StructField("error_message",     StringType(),    True),
    StructField("start_time",        TimestampType(), True),
    StructField("end_time",          TimestampType(), True),
    StructField("execution_seconds", DoubleType(),    True),
    StructField("batch_date",        DateType(),      True),
])

logger = get_logger(__name__)


def generate_run_id() -> str:
    """Generate a unique ID for this table run.

    uuid4() generates a random UUID — statistically impossible to collide.
    Format: '550e8400-e29b-41d4-a716-446655440000'

    run_id  = one table's run (unique per table per execution)
    batch_id = one job's run (shared across all tables in the same execution)
    """
    return str(uuid.uuid4())


def write_audit(
    spark: SparkSession,
    run_id: str,
    batch_id: str,
    config_id: int,
    source_system: str,
    source_table: str,
    target_table: str,
    status: str,                          # "started" | "success" | "failed"
    load_type: str,
    source_count: int = 0,
    records_inserted: int = 0,
    rejected_records: int = 0,
    validation_status: str = "pending",
    error_message: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> None:
    """Append one audit record to the audit Delta table.

    Uses spark.createDataFrame() to build a single-row DataFrame, then
    appends it. This keeps the audit write self-contained — no dependency
    on the DataFrame that was just ingested.
    """
    now = datetime.utcnow()
    execution_seconds = (
        (end_time - start_time).total_seconds()
        if start_time and end_time
        else None
    )

    # Build a plain Python dict — one row of audit data
    record = {
        "audit_id": abs(hash(run_id)) % (10 ** 9),  # deterministic int from UUID
        "run_id": run_id,
        "batch_id": batch_id,
        "config_id": config_id,
        "source_system": source_system,
        "source_table": source_table,
        "target_table": target_table,
        "status": status,
        "load_type": load_type,
        "source_count": source_count,
        "records_inserted": records_inserted,
        "rejected_records": rejected_records,
        "validation_status": validation_status,
        "error_message": error_message,
        "start_time": start_time or now,
        "end_time": end_time,
        "execution_seconds": execution_seconds,
        "batch_date": date.today(),
    }

    # Pass the explicit schema as the second argument.
    # Without it, Spark tries to infer types from Python values — but None
    # has no type, causing CANNOT_DETERMINE_TYPE on nullable fields.
    audit_df = spark.createDataFrame([record], schema=AUDIT_SCHEMA)

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(AUDIT_TABLE_PATH)
    )

    logger.info(
        "Audit written | run_id=%s | status=%s | source=%d | inserted=%d | rejected=%d",
        run_id[:8],  # first 8 chars of UUID is enough for log readability
        status,
        source_count,
        records_inserted,
        rejected_records,
    )
