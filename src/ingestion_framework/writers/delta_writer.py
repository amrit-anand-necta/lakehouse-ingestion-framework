"""Delta Lake writers — three strategies for writing data to bronze.

The three strategies and when to use each:

  full        → overwrite the entire table every run
                use for: small dimension/reference tables (products, branches)
                why: simple, always consistent, no state to manage

  append      → add new rows, never touch existing ones
                use for: immutable event data (logs, clicks, sensor readings)
                why: fastest write, no MERGE overhead, but can't handle updates

  incremental → Delta MERGE (upsert): update matched rows, insert new ones
                use for: transactional data where source rows get updated
                         (order status changes, payment updates)
                why: handles both inserts and updates in one atomic operation

Interview tip: "Why not always use MERGE?" — MERGE is the most expensive write.
It scans the target table to find matches. For a 10-billion-row fact table, that
scan costs real money. full/append avoid the scan entirely. Right strategy for
the right table type is a design decision, not a default.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

from ingestion_framework.config import IngestionConfig
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.session import WAREHOUSE_DIR

logger = get_logger(__name__)


def _target_path(config: IngestionConfig) -> str:
    """Build the absolute file path where this table's Delta files will live.

    Pattern: <warehouse>/<catalog>.db/<schema>/<table>
    This matches how Spark's local warehouse organises managed tables.
    """
    return f"{WAREHOUSE_DIR}/{config.target_catalog}.db/{config.target_schema}/{config.target_table}"


def write_full(df: DataFrame, config: IngestionConfig) -> int:
    """Overwrite the target Delta table with the entire incoming DataFrame.

    mode("overwrite") replaces ALL existing data.
    overwriteSchema=true allows the schema to change between runs — if the
    source adds a column, the target table schema updates automatically.

    Returns the number of rows written.
    """
    path = _target_path(config)
    row_count = df.count()

    logger.info(
        "Writing FULL | config_id=%d | target=%s | rows=%d",
        config.config_id, config.target_fqn, row_count,
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")   # schema can change between runs
        .save(path)
    )

    logger.info("FULL write complete | config_id=%d | rows=%d", config.config_id, row_count)
    return row_count


def write_append(df: DataFrame, config: IngestionConfig) -> int:
    """Append incoming rows to the target Delta table.

    mergeSchema=true means new columns in the source are added to the target
    rather than causing a schema mismatch error.

    Returns the number of rows written.
    """
    path = _target_path(config)
    row_count = df.count()

    logger.info(
        "Writing APPEND | config_id=%d | target=%s | rows=%d",
        config.config_id, config.target_fqn, row_count,
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")   # additive schema changes are safe
        .save(path)
    )

    logger.info("APPEND write complete | config_id=%d | rows=%d", config.config_id, row_count)
    return row_count


def write_incremental(df: DataFrame, config: IngestionConfig, spark: SparkSession) -> int:
    """Delta MERGE — upsert incoming rows into the target table.

    MERGE logic:
      WHEN MATCHED (same primary key exists in target)  → UPDATE all columns
      WHEN NOT MATCHED (new primary key)                → INSERT the row

    First-run handling: if the target table doesn't exist yet, fall back to
    write_full() to create it. The NEXT run will find the table and do a
    proper MERGE. This is standard practice — you can't MERGE into nothing.

    Returns the number of rows in the incoming DataFrame (all attempted).
    """
    path = _target_path(config)

    if not config.primary_key:
        raise ValueError(
            f"config_id={config.config_id}: primary_key is required for incremental load"
        )

    row_count = df.count()
    logger.info(
        "Writing INCREMENTAL (MERGE) | config_id=%d | target=%s | pk=%s | rows=%d",
        config.config_id, config.target_fqn, config.primary_key, row_count,
    )

    # Check if the target Delta table exists yet
    if not DeltaTable.isDeltaTable(spark, path):
        logger.info(
            "Target does not exist — first run, using full load | config_id=%d",
            config.config_id,
        )
        return write_full(df, config)

    # Load the existing Delta table as a DeltaTable object
    # DeltaTable.forPath() gives us the merge/update/delete API
    target = DeltaTable.forPath(spark, path)

    # Build a dynamic column map for the UPDATE SET and INSERT VALUES clauses.
    # For each column in the incoming DataFrame, we map:
    #   "column_name" -> "source.column_name"
    # This means: set the target column to the source's value for every column.
    col_map = {col: f"source.{col}" for col in df.columns}

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            # The merge condition: rows are "the same" when their PKs match
            f"target.{config.primary_key} = source.{config.primary_key}",
        )
        # WHEN MATCHED: a row with this PK already exists in the target → update it
        .whenMatchedUpdate(set=col_map)
        # WHEN NOT MATCHED: no row with this PK in the target → insert it
        .whenNotMatchedInsert(values=col_map)
        .execute()
    )

    logger.info("MERGE complete | config_id=%d | rows_attempted=%d", config.config_id, row_count)
    return row_count


def write_table(df: DataFrame, config: IngestionConfig, spark: SparkSession) -> int:
    """Dispatcher — routes to the correct write strategy based on load_type.

    This is the only function the pipeline imports from this module.
    Adding a new load_type = add one elif here + one new write function above.
    """
    if config.load_type == "full":
        return write_full(df, config)
    elif config.load_type == "append":
        return write_append(df, config)
    elif config.load_type == "incremental":
        return write_incremental(df, config, spark)
    else:
        raise ValueError(
            f"config_id={config.config_id}: unknown load_type '{config.load_type}'. "
            f"Expected: full | append | incremental"
        )
