"""Test all three write strategies end-to-end.

Run after bootstrap.py:
    python scripts/test_writer.py

What to observe:
  config_id=1 (products)      → full load    → 3 rows in bronze
  config_id=2 (customers)     → full load    → 4 rows (dedup removes 1)
  config_id=3 (transactions)  → incremental  → MERGE on first run = full load,
                                               re-run = MERGE (0 new rows since data unchanged)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.config import get_all_active_configs
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.readers.csv_reader import read_csv
from ingestion_framework.session import get_spark, WAREHOUSE_DIR
from ingestion_framework.validation import validate
from ingestion_framework.writers.delta_writer import write_table

logger = get_logger(__name__)


def main() -> None:
    spark = get_spark("test-writer")
    configs = get_all_active_configs(spark)

    for config in configs:
        logger.info("=== config_id=%d | %s | load_type=%s ===",
                    config.config_id, config.source_table, config.load_type)

        # Read
        df = read_csv(config, spark)

        # Validate
        result = validate(df, config, spark)
        if result.status == "failed":
            logger.error("Validation failed — skipping write | config_id=%d", config.config_id)
            continue

        # Write
        rows_written = write_table(result.df, config, spark)

        # Read back from Delta to confirm it landed correctly
        target_path = f"{WAREHOUSE_DIR}/{config.target_catalog}.db/{config.target_schema}/{config.target_table}"
        written_df = spark.read.format("delta").load(target_path)
        logger.info("Bronze table row count: %d", written_df.count())
        written_df.show(truncate=False)


if __name__ == "__main__":
    main()
