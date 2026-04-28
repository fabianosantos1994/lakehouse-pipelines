from collections.abc import Callable

from pyspark.sql import SparkSession

from common.logger import get_logger
from common.spark import build_spark_session


def run_job(app_name: str, job_fn: Callable[[SparkSession], None]) -> None:
    spark = None
    logger = get_logger(app_name)

    logger.info("Job started")

    try:
        spark = build_spark_session(app_name)
        job_fn(spark)
    except Exception:
        logger.exception("Job failed")
        raise
    else:
        logger.info("Job succeeded")
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")
