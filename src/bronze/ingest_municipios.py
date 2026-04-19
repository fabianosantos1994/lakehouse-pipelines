from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, trim
from pyspark.sql.types import StructType, StructField, StringType


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_ingest_municipios")

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

    spark.stop()


if __name__ == "__main__":
    main()