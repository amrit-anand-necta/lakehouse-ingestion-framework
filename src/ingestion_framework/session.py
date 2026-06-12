"""SparkSession factory with Delta Lake enabled.

A single place to create the session means every entry point (orchestrator,
bootstrap script, tests) gets identical Spark behaviour. On Databricks the
session already exists — this factory is what makes the framework runnable
locally too.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ingestion_framework.logging_setup import get_logger

logger = get_logger(__name__)

WAREHOUSE_DIR = "data/lakehouse"


def get_spark(app_name: str = "ingestion-framework") -> SparkSession:
    """Build (or reuse) a Delta-enabled SparkSession.

    ``SparkSession.builder.getOrCreate()`` returns the existing session if one
    is already running — safe to call from anywhere in the codebase.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Delta Lake wiring: register Delta's SQL extensions and catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Local lakehouse location for managed tables
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # Sensible local defaults: 200 shuffle partitions (the default) is
        # wasteful on a laptop-sized dataset
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready | app=%s | spark=%s", app_name, spark.version)
    return spark
