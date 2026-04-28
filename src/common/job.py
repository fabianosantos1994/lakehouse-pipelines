from collections.abc import Callable

from pyspark.sql import SparkSession

from common.spark import build_spark_session


def run_job(app_name: str, job_fn: Callable[[SparkSession], None]) -> None:
    spark = build_spark_session(app_name)

    try:
        job_fn(spark)
    finally:
        spark.stop()
