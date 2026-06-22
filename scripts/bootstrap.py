"""Bootstrap script — creates config + audit Delta tables and seeds sample data.

Run once before any other script:
    python scripts/bootstrap.py

Safe to re-run: CREATE IF NOT EXISTS + INSERT IGNORE logic means it won't
duplicate rows if you run it twice.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_framework.logging_setup import get_logger
from ingestion_framework.session import get_spark

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Sample source files — written to data/landing/ so readers have real input
# ---------------------------------------------------------------------------

SAMPLE_CUSTOMERS = """customer_id,name,city,signup_date
1,Ravi Kumar,Mumbai,2024-01-10
2,Priya Sharma,Delhi,2024-02-15
3,Arjun Nair,Bangalore,2024-03-20
4,Sneha Patel,Ahmedabad,2024-01-25
1,Ravi Kumar,Mumbai,2024-01-10
"""
# Note: customer_id=1 appears twice — intentional duplicate to test dedup gate

SAMPLE_TRANSACTIONS = """txn_id,customer_id,amount,txn_date,status
101,1,5000.00,2024-04-01,completed
102,2,1200.50,2024-04-02,completed
103,3,8750.00,2024-04-03,pending
104,1,300.00,2024-04-04,completed
105,4,9999.99,2024-04-05,failed
"""

SAMPLE_PRODUCTS = """product_id,product_name,category,price
P001,Home Loan,Lending,0.00
P002,Credit Card,Cards,0.00
P003,Savings Account,Deposits,0.00
"""


def create_landing_files() -> None:
    """Write CSV files that the CSV reader will ingest."""
    landing = Path("data/landing")
    landing.mkdir(parents=True, exist_ok=True)

    (landing / "customers").mkdir(exist_ok=True)
    (landing / "customers" / "customers.csv").write_text(SAMPLE_CUSTOMERS)

    (landing / "transactions").mkdir(exist_ok=True)
    (landing / "transactions" / "transactions.csv").write_text(SAMPLE_TRANSACTIONS)

    (landing / "products").mkdir(exist_ok=True)
    (landing / "products" / "products.csv").write_text(SAMPLE_PRODUCTS)

    logger.info("Landing files written to data/landing/")


def create_config_table(spark) -> None:
    """Create the Delta config table and insert sample pipeline configs."""
    spark.sql("CREATE DATABASE IF NOT EXISTS ingestion_framework")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ingestion_framework.config_table (
            config_id       INT,
            source_system   STRING,
            source_type     STRING,
            domain          STRING,
            datasource      STRING,
            source_schema   STRING,
            source_table    STRING,
            source_path     STRING,
            delimiter       STRING,
            header_flag     STRING,
            infer_schema    STRING,
            sheet_name      STRING,
            primary_key     STRING,
            load_type       STRING,
            ingestion_type  STRING,
            partition_column  STRING,
            num_partitions    INT,
            incremental_key_1   STRING,
            incremental_value_1 STRING,
            incremental_key_2   STRING,
            incremental_value_2 STRING,
            target_catalog  STRING,
            target_schema   STRING,
            target_table    STRING,
            is_active       STRING,
            created_at      TIMESTAMP,
            updated_at      TIMESTAMP
        )
        USING DELTA
    """)
    logger.info("Config table ready")

    # Check if already seeded — idempotent re-runs
    count = spark.table("ingestion_framework.config_table").count()
    if count > 0:
        logger.info("Config table already has %d rows — skipping seed", count)
        return

    spark.sql("""
        INSERT INTO ingestion_framework.config_table VALUES
        -- config_id=1: full load, CSV, dimension-style (products — small, no PK updates expected)
        (1, 'filesystem', 'csv', 'product', 'local_fs',
         NULL, 'products', 'data/landing/products',
         ',', 'true', 'true', NULL,
         'product_id', 'full', 'batch',
         NULL, 1,
         NULL, NULL, NULL, NULL,
         'local', 'bronze', 'products',
         'Y', current_timestamp(), current_timestamp()),

        -- config_id=2: full load, CSV, dimension-style (customers)
        (2, 'filesystem', 'csv', 'customer', 'local_fs',
         NULL, 'customers', 'data/landing/customers',
         ',', 'true', 'true', NULL,
         'customer_id', 'full', 'batch',
         NULL, 1,
         NULL, NULL, NULL, NULL,
         'local', 'bronze', 'customers',
         'Y', current_timestamp(), current_timestamp()),

        -- config_id=3: incremental load, CSV, fact-style (transactions grow daily)
        (3, 'filesystem', 'csv', 'finance', 'local_fs',
         NULL, 'transactions', 'data/landing/transactions',
         ',', 'true', 'true', NULL,
         'txn_id', 'incremental', 'batch',
         NULL, 1,
         'txn_date', '2024-03-31', NULL, NULL,
         'local', 'bronze', 'transactions',
         'Y', current_timestamp(), current_timestamp())
    """)
    logger.info("Config table seeded with 3 pipeline configs")


def create_audit_table(spark) -> None:
    """Create the audit table — one row per pipeline run event."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ingestion_framework.audit_table (
            audit_id            BIGINT,
            run_id              STRING,
            batch_id            STRING,
            config_id           INT,
            source_system       STRING,
            source_table        STRING,
            target_table        STRING,
            status              STRING,
            load_type           STRING,
            source_count        BIGINT,
            records_inserted    BIGINT,
            rejected_records    BIGINT,
            validation_status   STRING,
            error_message       STRING,
            start_time          TIMESTAMP,
            end_time            TIMESTAMP,
            execution_seconds   DOUBLE,
            batch_date          DATE
        )
        USING DELTA
    """)
    logger.info("Audit table ready")


def main() -> None:
    spark = get_spark("bootstrap")
    create_landing_files()
    create_config_table(spark)
    create_audit_table(spark)
    logger.info("Bootstrap complete ✅")
    logger.info("Config rows: %d", spark.table("ingestion_framework.config_table").count())
    spark.table("ingestion_framework.config_table").show(truncate=False)


if __name__ == "__main__":
    main()
