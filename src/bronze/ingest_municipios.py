import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, trim
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.job import run_job


def run_pipeline(spark: SparkSession) -> None:
    schema = StructType([
        StructField("municipio_id", StringType(), True),
        StructField("municipio_name", StringType(), True),
        StructField("state", StringType(), True),
    ])

    input_path = "/opt/project/datasets/raw/municipios.csv"
    target_table = "lakehouse.bronze.municipios"

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

    (
        df.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"Successfully wrote data to {target_table}")
    print(f"Row count: {df.count()}")


if __name__ == "__main__":
    run_job("bronze_ingest_municipios", run_pipeline)
