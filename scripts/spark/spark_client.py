from contextlib import contextmanager

from pyspark.sql import SparkSession

# from spark.jars.jars_enum import SPARK_JARS

# JARS = f"{SPARK_JARS['GCS']['path']}:{SPARK_JARS['BigQuery']['path']}"


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
