"""Step-1 smoke test: verify Spark + Delta + logging all work locally.

Run from the repo root:
    python scripts/smoke_test.py

Expected: a Delta table gets written and read back, and you see framework
log lines. If this passes, the foundation is solid.
"""

import sys
from pathlib import Path

# Make src/ importable when running as a plain script (before we add pyproject.toml)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.logging_setup import get_logger
from ingestion_framework.session import get_spark

logger = get_logger(__name__)


def main() -> None:
    spark = get_spark("smoke-test")

    df = spark.createDataFrame(
        [(1, "csv"), (2, "jdbc"), (3, "api")],
        schema="source_id INT, source_type STRING",
    )
    logger.info("Created test dataframe with %d rows", df.count())

    out = "data/lakehouse/_smoke_test"
    df.write.format("delta").mode("overwrite").save(out)
    logger.info("Wrote Delta table to %s", out)

    back = spark.read.format("delta").load(out)
    assert back.count() == 3, "row count mismatch after Delta round-trip"
    back.show()
    logger.info("SMOKE TEST PASSED ✅ — Spark %s + Delta working", spark.version)


if __name__ == "__main__":
    main()
