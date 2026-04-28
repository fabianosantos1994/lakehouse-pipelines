from pyspark.sql import SparkSession

from common.config import get_spark_config


def build_spark_session(app_name: str) -> SparkSession:
    config = get_spark_config()
    catalog = config.catalog

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", catalog)
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hive")
        .config(f"spark.sql.catalog.{catalog}.uri", config.hive_metastore_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", config.warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", config.s3_path_style_access)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3_ssl_enabled)
        .getOrCreate()
    )
