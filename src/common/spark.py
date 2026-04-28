import os

from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    catalog = os.getenv("LAKEHOUSE_CATALOG", "lakehouse")
    warehouse = os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse/warehouse")
    hive_metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minio")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minio123")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", catalog)
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hive")
        .config(f"spark.sql.catalog.{catalog}.uri", hive_metastore_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
