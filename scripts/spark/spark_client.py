from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def spark_session(app_name: str = "spark_session"):
    spark = (
        SparkSession.builder.appName(app_name)
        # .config("spark.driver.extraClassPath", JARS)
        .getOrCreate()
    )

    try:
        yield spark
    finally:
        spark.stop()
