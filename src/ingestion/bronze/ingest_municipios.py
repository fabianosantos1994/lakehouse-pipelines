import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, trim
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(str(Path(__file__).resolve().parents[2]))

from common.job import run_job
from common.logger import get_logger
from configs.municipios import INPUT_PATH, BRONZE_TABLE

logger = get_logger(__name__)


def run_pipeline(spark: SparkSession) -> None:
    schema = StructType([
        StructField("municipio_id", StringType(), True),
        StructField("municipio_name", StringType(), True),
        StructField("state", StringType(), True),
    ])

    input_path = INPUT_PATH
    target_table = BRONZE_TABLE

    logger.info("Source path: %s", input_path)
    logger.info("Target table: %s", target_table)

    logger.info("Reading source data")
    df = (
        spark.read
        .option("header", "false")
        .option("sep", ";")
        .schema(schema)
        .csv(input_path)
        .select(
            trim(col("municipio_id")).alias("municipio_id"),
            trim(col("municipio_name")).alias("municipio_name"),
            trim(col("state")).alias("state"),
        )
        .withColumn("ingestion_date", current_date())
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

    logger.info("Writing target data")
    (
        df.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    rows_written = df.count()
    logger.info("Rows written: %s", rows_written)


if __name__ == "__main__":
    run_job("bronze_ingest_municipios", run_pipeline)
