"""Manual test for the CSV reader.

Run after bootstrap.py:
    python scripts/test_csv_reader.py

You should see all 3 CSVs read successfully with correct row counts and types.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.config import get_all_active_configs
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.readers.csv_reader import read_csv
from ingestion_framework.session import get_spark

logger = get_logger(__name__)


def main() -> None:
    spark = get_spark("test-csv-reader")

    configs = get_all_active_configs(spark)

    for config in configs:
        logger.info("--- Testing config_id=%d | table=%s ---", config.config_id, config.source_table)
        df = read_csv(config, spark)

        # printSchema shows the column names AND their detected types
        # this is what inferSchema bought us — not everything is StringType
        df.printSchema()
        df.show(truncate=False)


if __name__ == "__main__":
    main()
