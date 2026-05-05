from collections.abc import Callable
from time import perf_counter

from pyspark.sql import SparkSession

from common.logger import get_logger
from common.spark import build_spark_session


def run_job(app_name: str, job_fn: Callable[[SparkSession], None]) -> None:
    spark = None
    logger = get_logger(app_name)
    started_at = perf_counter()

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
        duration_seconds = perf_counter() - started_at
        logger.info("Job duration: %.2f seconds", duration_seconds)

        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")
