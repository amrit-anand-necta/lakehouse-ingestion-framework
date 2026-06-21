"""Manual test for validation gates.

Run after bootstrap.py:
    python scripts/test_validation.py

What to look for:
- customers: 1 duplicate (customer_id=1 appears twice) should be dropped → 4 final rows
- products: clean data, no rejects, no dupes → 3 final rows
- transactions: clean data → 5 final rows
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.config import get_all_active_configs
from ingestion_framework.logging_setup import get_logger
from ingestion_framework.readers.csv_reader import read_csv
from ingestion_framework.session import get_spark
from ingestion_framework.validation import validate

logger = get_logger(__name__)


def main() -> None:
    spark = get_spark("test-validation")
    configs = get_all_active_configs(spark)

    for config in configs:
        logger.info("=== config_id=%d | %s ===", config.config_id, config.source_table)
        df = read_csv(config, spark)
        result = validate(df, config, spark)
        logger.info(
            "Result | source=%d | rejected=%d | final=%d | status=%s",
            result.source_count, result.rejected_count,
            result.final_count, result.status,
        )
        result.df.show(truncate=False)


if __name__ == "__main__":
    main()
