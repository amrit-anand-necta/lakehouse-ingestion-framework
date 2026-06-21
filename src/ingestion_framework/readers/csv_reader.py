"""CSV reader — reads one or more CSV files from a folder into a Spark DataFrame.

Design decisions worth knowing for interviews:

1. We read the entire FOLDER, not a single file.
   In production, new files land daily in the same folder. Reading the folder
   means the pipeline picks up all files automatically — no path changes needed.

2. All options (delimiter, header, inferSchema) come from the config row.
   The reader has zero hardcoded values. Same function handles comma-separated,
   pipe-separated, tab-separated files — just change the config.

3. inferSchema=True makes Spark scan the file once to detect column types.
   It costs an extra pass but saves you from everything landing as StringType.
   For large files you'd define an explicit schema instead — faster and safer.
"""

from pyspark.sql import DataFrame, SparkSession

from ingestion_framework.config import IngestionConfig
from ingestion_framework.logging_setup import get_logger

logger = get_logger(__name__)


def read_csv(config: IngestionConfig, spark: SparkSession) -> DataFrame:
    """Read all CSV files in config.source_path into a single DataFrame.

    Args:
        config: the pipeline's config row (contains path, delimiter, etc.)
        spark:  the active SparkSession

    Returns:
        DataFrame with all rows from all CSV files in the source folder.
        Columns are named exactly as they appear in the CSV header.

    Raises:
        ValueError: if source_path is missing from the config.
    """
    if not config.source_path:
        raise ValueError(
            f"config_id={config.config_id}: source_path is required for CSV reader"
        )

    logger.info(
        "Reading CSV | config_id=%d | path=%s | delimiter='%s' | header=%s | infer=%s",
        config.config_id,
        config.source_path,
        config.delimiter,
        config.header_flag,
        config.infer_schema,
    )

    df = (
        spark.read
        # format("csv") tells Spark to use the built-in CSV parser
        .format("csv")
        # header=true means row 1 is column names, not data
        .option("header", config.header_flag or "true")
        # sep is the column separator — comma by default, but pipe/tab work too
        .option("sep", config.delimiter or ",")
        # inferSchema makes Spark detect int/double/date types automatically
        # trade-off: costs one extra scan, but columns aren't all strings
        .option("inferSchema", config.infer_schema or "true")
        # nullValue: treat the string "null" or "NULL" in files as SQL NULL
        .option("nullValue", "null")
        # multiLine: handle fields that contain newlines inside quotes
        .option("multiLine", "true")
        # mode=PERMISSIVE (default): bad rows get NULLs instead of crashing
        # alternative: DROPMALFORMED silently drops bad rows, FAILFAST crashes
        .option("mode", "PERMISSIVE")
        # path ends with / so Spark reads ALL files in the folder
        .load(config.source_path)
    )

    row_count = df.count()
    logger.info(
        "CSV read complete | config_id=%d | rows=%d | columns=%s",
        config.config_id,
        row_count,
        df.columns,
    )
    return df
