import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when
from pyspark.sql.types import IntegerType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.job import run_job


def run_pipeline(spark: SparkSession) -> None:
    source_table = "lakehouse.bronze.municipios"
    target_table = "lakehouse.silver.municipios"

    df = spark.table(source_table)

    df_clean = (
        df
        .filter(col("municipio_id").isNotNull())
        .withColumn("municipio_id", col("municipio_id").cast(IntegerType()))
        .withColumn("municipio_name", upper(trim(col("municipio_name"))))
        .withColumn("state", upper(trim(col("state"))))
        .withColumn(
            "region",
            when(col("state").isin("SP", "RJ", "MG", "ES"), "SUDESTE")
            .when(col("state").isin("RS", "SC", "PR"), "SUL")
            .when(col("state").isin("GO", "MT", "MS", "DF"), "CENTRO_OESTE")
            .when(col("state").isin("BA", "PE", "CE", "MA", "PB", "PI", "RN", "AL", "SE"), "NORDESTE")
            .when(col("state").isin("AM", "PA", "AC", "AP", "RO", "RR", "TO"), "NORTE")
            .otherwise("UNKNOWN")
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    (
        df_clean.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"Successfully wrote data to {target_table}")
    print(f"Row count: {df_clean.count()}")


if __name__ == "__main__":
    run_job("silver_municipios", run_pipeline)
