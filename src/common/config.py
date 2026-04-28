import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkConfig:
    catalog: str
    warehouse: str
    hive_metastore_uri: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_path_style_access: str
    s3_ssl_enabled: str


def get_spark_config() -> SparkConfig:
    return SparkConfig(
        catalog=os.getenv("LAKEHOUSE_CATALOG", "lakehouse"),
        warehouse=os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse/warehouse"),
        hive_metastore_uri=os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"),
        s3_endpoint=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        s3_access_key=os.getenv("S3_ACCESS_KEY", "minio"),
        s3_secret_key=os.getenv("S3_SECRET_KEY", "minio123"),
        s3_path_style_access=os.getenv("S3_PATH_STYLE_ACCESS", "true"),
        s3_ssl_enabled=os.getenv("S3_SSL_ENABLED", "false"),
    )
