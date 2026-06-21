"""Validation gates — run on every DataFrame before it touches bronze.

Philosophy: soft-fail.
Bad rows are counted and reported in the audit table, but they don't crash
the pipeline. 9999 good rows should still land even if 1 row is malformed.
The only hard-fail is if ALL rows are rejected — writing an empty table is
usually a sign of something badly wrong (wrong path, empty source file).

Three gates in order:
  1. Null primary key rejection  — rows without a PK can't be merged/deduped
  2. Deduplication               — within this batch, last-write-wins per PK
  3. Schema drift detection      — log new/removed columns vs existing target
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion_framework.config import IngestionConfig
from ingestion_framework.logging_setup import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Everything the audit table needs to know about validation.

    A dataclass again — same reason as IngestionConfig: typed, readable,
    no boilerplate constructor.
    """
    df: DataFrame           # the cleaned DataFrame ready for writing
    source_count: int       # rows read from source (before any rejection)
    rejected_count: int     # rows dropped by null-PK gate
    final_count: int        # rows that will actually be written
    status: str             # "passed" | "warning" | "failed"
    drift_columns: list     # new columns found vs existing target schema


# ---------------------------------------------------------------------------
# Gate 1 — Null primary key rejection
# ---------------------------------------------------------------------------

def _reject_null_pk(df: DataFrame, config: IngestionConfig) -> tuple[DataFrame, int]:
    """Drop rows where the primary key column is null.

    Returns the clean DataFrame and the count of rejected rows.

    Why this matters: Delta MERGE uses the primary key to match rows.
    A null key can't match anything — the row either becomes a phantom
    insert or causes a non-deterministic MERGE. Safer to reject it explicitly.
    """
    if not config.primary_key:
        # No PK configured — skip this gate (e.g. append-only event tables)
        return df, 0

    # F.col() creates a Column object from a string name.
    # isNull() returns True when the value is SQL NULL.
    total_before = df.count()
    clean_df = df.filter(F.col(config.primary_key).isNotNull())
    rejected = total_before - clean_df.count()

    if rejected > 0:
        logger.warning(
            "Null PK rejected | config_id=%d | pk=%s | rejected=%d",
            config.config_id, config.primary_key, rejected,
        )
    return clean_df, rejected


# ---------------------------------------------------------------------------
# Gate 2 — Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(df: DataFrame, config: IngestionConfig) -> DataFrame:
    """Keep only one row per primary key value within this batch.

    Strategy: last-write-wins using row_number() over a Window partitioned
    by the primary key. If the source has a natural ordering column (like
    updated_at), we'd order by that descending so the most recent row wins.
    Here we use monotonically_increasing_id() as a tiebreaker when there's
    no ordering column — deterministic within a batch even without timestamps.

    Window functions are a very common interview topic. The pattern here:
      PARTITION BY pk_col  → treat each pk value as a separate group
      ORDER BY tiebreaker  → within each group, define what "last" means
      row_number() = 1     → keep only the first row in each group
    """
    if not config.primary_key:
        return df

    # monotonically_increasing_id() assigns a unique long integer to each row.
    # It's NOT sequential (gaps between partitions) but it IS unique, which is
    # all we need for tiebreaking.
    df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())

    window = Window.partitionBy(config.primary_key).orderBy(F.col("_row_id").desc())
    df_ranked = df_with_id.withColumn("_rn", F.row_number().over(window))

    # Keep only rank 1 (the "last" row per PK), then drop helper columns
    clean_df = df_ranked.filter(F.col("_rn") == 1).drop("_row_id", "_rn")

    dupes = df.count() - clean_df.count()
    if dupes > 0:
        logger.warning(
            "Duplicates dropped | config_id=%d | pk=%s | dupes=%d",
            config.config_id, config.primary_key, dupes,
        )
    return clean_df


# ---------------------------------------------------------------------------
# Gate 3 — Schema drift detection
# ---------------------------------------------------------------------------

def _detect_schema_drift(
    df: DataFrame,
    config: IngestionConfig,
    spark: SparkSession,
) -> list[str]:
    """Compare incoming columns against the existing target Delta table.

    Returns a list of new column names (columns in the source that don't exist
    in the target yet). An empty list means no drift.

    This is non-blocking — we log the drift and let the write proceed.
    Delta's mergeSchema=true handles additive drift automatically.
    The audit record captures the drift list for ops visibility.

    Why non-blocking? Blocking on schema drift would halt the pipeline every
    time a source adds a harmless new column. That's too noisy in production.
    Blocking makes sense for REMOVED or TYPE-CHANGED columns — that's a
    future enhancement.
    """
    from ingestion_framework.session import WAREHOUSE_DIR

    target_path = f"{WAREHOUSE_DIR}/{config.target_catalog}.db/{config.target_schema}/{config.target_table}"

    try:
        existing = spark.read.format("delta").load(target_path)
        existing_cols = set(existing.columns)
        incoming_cols = set(df.columns)
        new_cols = list(incoming_cols - existing_cols)
        if new_cols:
            logger.warning(
                "Schema drift detected | config_id=%d | new_columns=%s",
                config.config_id, new_cols,
            )
        return new_cols
    except Exception:
        # Target doesn't exist yet (first run) — no drift to detect
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def validate(
    df: DataFrame,
    config: IngestionConfig,
    spark: SparkSession,
) -> ValidationResult:
    """Run all validation gates and return a ValidationResult.

    This is the only function the pipeline imports from this module —
    one call, everything happens inside.
    """
    source_count = df.count()
    logger.info(
        "Validation start | config_id=%d | source_rows=%d",
        config.config_id, source_count,
    )

    # Gate 1: null PK
    df, rejected = _reject_null_pk(df, config)

    # Gate 2: deduplication
    df = _deduplicate(df, config)
    final_count = df.count()

    # Gate 3: schema drift (non-blocking, just for logging/audit)
    drift_cols = _detect_schema_drift(df, config, spark)

    # Determine overall validation status
    if final_count == 0:
        status = "failed"     # nothing to write — something is badly wrong
    elif rejected > 0 or drift_cols:
        status = "warning"    # wrote something, but with issues worth noting
    else:
        status = "passed"

    logger.info(
        "Validation done | config_id=%d | status=%s | source=%d | rejected=%d | final=%d | drift=%s",
        config.config_id, status, source_count, rejected, final_count, drift_cols,
    )

    return ValidationResult(
        df=df,
        source_count=source_count,
        rejected_count=rejected,
        final_count=final_count,
        status=status,
        drift_columns=drift_cols,
    )
