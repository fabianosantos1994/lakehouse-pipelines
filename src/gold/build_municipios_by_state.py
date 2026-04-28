import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.job import run_job


def run_pipeline(spark: SparkSession) -> None:
    source_table = "lakehouse.silver.municipios"
    target_table = "lakehouse.gold.municipios_by_state"

    df_silver = spark.table(source_table)

    df_gold = (
        df_silver
        .groupBy(col("state"))
        .agg(
            count("*").alias("municipality_count"),
            spark_max("ingestion_date").alias("reference_ingestion_date"),
        )
        .orderBy(col("municipality_count").desc(), col("state").asc())
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    (
        df_gold.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"Successfully wrote data to {target_table}")
    print(f"Row count: {df_gold.count()}")


if __name__ == "__main__":
    run_job("gold_build_municipios_by_state", run_pipeline)
