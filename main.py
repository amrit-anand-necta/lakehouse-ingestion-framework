"""Entry point — run the full ingestion framework.

Usage:
    python main.py

Runs all active pipelines from the config table, writes results to bronze
Delta tables, and writes audit records for every run.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from ingestion_framework.orchestrator import run_all
from ingestion_framework.session import get_spark

if __name__ == "__main__":
    spark = get_spark("lakehouse-ingestion-framework")
    run_all(spark)
