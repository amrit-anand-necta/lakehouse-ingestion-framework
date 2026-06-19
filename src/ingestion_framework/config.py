"""Config table access.

The config table is a Delta table with one row per source-to-target pipeline.
The orchestrator reads all active rows; the pipeline reads one row by config_id.

Why Delta for the config table?
- ACID updates: watermark updates after incremental runs are single-row UPDATEs
  that either fully commit or don't happen at all — no partial state corruption.
- Time travel: if a bad config is written, RESTORE to the previous version.
- Same stack as everything else — no extra dependency (SQLite, Postgres, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession

from ingestion_framework.logging_setup import get_logger

logger = get_logger(__name__)

CONFIG_TABLE = "ingestion_framework.config_table"
AUDIT_TABLE = "ingestion_framework.audit_table"


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------

@dataclass
class IngestionConfig:
    """One row of the config table, typed and validated.

    A dataclass gives us:
    - auto-generated __init__ and __repr__ (no boilerplate)
    - type hints that document what each field is
    - dot-access (config.source_type) instead of dict['source_type']

    Optional fields default to None so callers don't have to supply them
    unless the source type actually needs them.
    """

    # Identity
    config_id: int
    source_system: str
    source_type: str          # csv | json | parquet | jdbc | api
    domain: str
    datasource: str           # secret key prefix for JDBC / API credentials

    # Source location
    source_schema: Optional[str] = None
    source_table: Optional[str] = None
    source_path: Optional[str] = None

    # File-format options (only for file-based sources)
    delimiter: Optional[str] = ","
    header_flag: Optional[str] = "true"
    infer_schema: Optional[str] = "true"
    sheet_name: Optional[str] = None

    # Load behaviour
    primary_key: Optional[str] = None
    load_type: str = "full"         # full | append | incremental
    ingestion_type: str = "batch"

    # Partitioning (JDBC parallel reads + Delta partition)
    partition_column: Optional[str] = None
    num_partitions: int = 1

    # Watermark (incremental loads only)
    incremental_key_1: Optional[str] = None
    incremental_value_1: Optional[str] = None
    incremental_key_2: Optional[str] = None
    incremental_value_2: Optional[str] = None

    # Target
    target_catalog: str = "local"
    target_schema: str = "bronze"
    target_table: str = ""

    # Operational
    is_active: str = "Y"

    @property
    def target_fqn(self) -> str:
        """Fully-qualified target table name: catalog.schema.table.

        Using a property means callers write ``config.target_fqn`` — readable
        and computed once per access. Equivalent to ``_target_fqn(config)``
        in your kotak notebooks, but attached to the object itself.
        """
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    @property
    def has_watermark(self) -> bool:
        """True when this config has at least one incremental watermark column."""
        return bool(self.incremental_key_1 and self.incremental_value_1)

    @classmethod
    def from_row(cls, row) -> "IngestionConfig":
        """Build an IngestionConfig from a Spark Row.

        Spark Row has no .get() method — accessing a missing field raises
        AttributeError. ``_safe()`` handles that, returning the default
        instead of crashing. This is the same pattern as your kotak
        ``_safe(row, key, default)`` utility.
        """
        return cls(
            config_id=_safe(row, "config_id", 0),
            source_system=_safe(row, "source_system", ""),
            source_type=_safe(row, "source_type", "").lower(),
            domain=_safe(row, "domain", ""),
            datasource=_safe(row, "datasource", ""),
            source_schema=_safe(row, "source_schema", None),
            source_table=_safe(row, "source_table", None),
            source_path=_safe(row, "source_path", None),
            delimiter=_safe(row, "delimiter", ","),
            header_flag=_safe(row, "header_flag", "true"),
            infer_schema=_safe(row, "infer_schema", "true"),
            sheet_name=_safe(row, "sheet_name", None),
            primary_key=_safe(row, "primary_key", None),
            load_type=_safe(row, "load_type", "full").lower(),
            ingestion_type=_safe(row, "ingestion_type", "batch").lower(),
            partition_column=_safe(row, "partition_column", None),
            num_partitions=int(_safe(row, "num_partitions", 1) or 1),
            incremental_key_1=_safe(row, "incremental_key_1", None),
            incremental_value_1=_safe(row, "incremental_value_1", None),
            incremental_key_2=_safe(row, "incremental_key_2", None),
            incremental_value_2=_safe(row, "incremental_value_2", None),
            target_catalog=_safe(row, "target_catalog", "local"),
            target_schema=_safe(row, "target_schema", "bronze"),
            target_table=_safe(row, "target_table", ""),
            is_active=_safe(row, "is_active", "Y"),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(row, key: str, default):
    """Safe Spark Row accessor — returns default if field missing or null-like.

    Spark Row.__getattr__ raises AttributeError for missing columns, and some
    optional fields may be stored as the string "null" rather than SQL NULL.
    This function guards both cases.
    """
    try:
        val = getattr(row, key)
        if val is None or str(val).lower() == "null":
            return default
        return val
    except AttributeError:
        return default


# ---------------------------------------------------------------------------
# Table access functions
# ---------------------------------------------------------------------------

def get_config(spark: SparkSession, config_id: int) -> IngestionConfig:
    """Fetch a single config row by ID.

    Raises ValueError if the config_id doesn't exist — better to fail fast
    with a clear message than let a None propagate and crash deep in the
    pipeline with a confusing AttributeError.
    """
    rows = (
        spark.table(CONFIG_TABLE)
        .filter(f"config_id = {config_id}")
        .collect()
    )
    if not rows:
        raise ValueError(f"No config found for config_id={config_id}")
    logger.info("Loaded config | config_id=%d | source=%s | target=%s",
                config_id, rows[0].source_type, rows[0].target_table)
    return IngestionConfig.from_row(rows[0])


def get_all_active_configs(spark: SparkSession) -> list[IngestionConfig]:
    """Return all configs where is_active = 'Y', ordered by config_id.

    The orchestrator calls this once per run. Ordering by config_id makes
    execution order deterministic — easier to follow in logs.
    """
    rows = (
        spark.table(CONFIG_TABLE)
        .filter("is_active = 'Y'")
        .orderBy("config_id")
        .collect()
    )
    configs = [IngestionConfig.from_row(r) for r in rows]
    logger.info("Loaded %d active configs", len(configs))
    return configs


def update_watermark(
    spark: SparkSession,
    config_id: int,
    new_value_1: str,
    new_value_2: Optional[str] = None,
) -> None:
    """Update the watermark values in the config table after a successful incremental run.

    This is the key to automated incremental loading: after each run we store
    the MAX value of the watermark column so the next run picks up from there.

    Uses a SQL UPDATE on the Delta config table — ACID guaranteed.
    """
    set_clause = f"incremental_value_1 = '{new_value_1}'"
    if new_value_2:
        set_clause += f", incremental_value_2 = '{new_value_2}'"

    spark.sql(f"""
        UPDATE {CONFIG_TABLE}
        SET {set_clause}, updated_at = current_timestamp()
        WHERE config_id = {config_id}
    """)
    logger.info("Updated watermark | config_id=%d | new_value_1=%s", config_id, new_value_1)
