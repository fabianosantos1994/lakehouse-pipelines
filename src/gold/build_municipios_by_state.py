from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_build_municipios_by_state")
        # Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        # S3 / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()

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

    spark.stop()


if __name__ == "__main__":
    main()