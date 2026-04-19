from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when
from pyspark.sql.types import IntegerType


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_municipios")

        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "lakehouse")

        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")

        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()

    source_table = "lakehouse.bronze.municipios"
    target_table = "lakehouse.silver.municipios"

    df = spark.table(source_table)

    df_clean = (
        df
        .filter(col("municipio_id").isNotNull())
        .withColumn("municipio_id", col("municipio_id").cast(IntegerType()))
        .withColumn("municipio_name", upper(trim(col("municipio_name"))))
        .withColumn("state", upper(trim(col("state"))))

        # exemplo simples de derivação
        .withColumn(
            "region",
            when(col("state").isin("SP", "RJ", "MG", "ES"), "SUDESTE")
            .when(col("state").isin("RS", "SC", "PR"), "SUL")
            .otherwise("OUTROS")
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

    spark.stop()


if __name__ == "__main__":
    main()