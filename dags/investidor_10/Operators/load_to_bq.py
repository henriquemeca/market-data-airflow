import select
from typing import Any, List, Union

from airflow.utils.context import Context
from helpers.spark.jars.jars_enum import SPARK_JARS
from helpers.spark.spark_client import spark_session
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import arrays_zip, col, explode
from pyspark.sql.types import ArrayType, StructField, StructType

JARS = f"{SPARK_JARS['GCS']['path']}:{SPARK_JARS['BigQuery']['path']}"


import json
import logging
import random
from typing import Dict, Tuple

import aiohttp
import pandas as pd
from airflow.models import BaseOperator
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.utils.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    RAW_BUCKET,
)


class LoadJsonToBigQuery(BaseOperator):
    """ """

    def __init__(
        self,
        data_source: str,
        bq_temp_bucket: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.data_source = data_source
        self.bq_temp_bucket = bq_temp_bucket

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read.option("multiline", "true").json(
            f"./investidor_10/{self.data_source}/*.json"
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return self.__flatten_arrays_and_structs(df)

    def load(self, df: DataFrame) -> None:
        bq_options = {
            "dataset": "investidor_10",
            "table": self.data_source,
            "temporaryGcsBucket": self.bq_temp_bucket,
        }

        df.write.format("bigquery").options(**bq_options).mode("overwrite").save()

    def execute(self, **context) -> Any:
        with spark_session(self.data_source) as spark:
            self.load(self.transform(self.read(spark)))

    def __type_is_in_df(self, df: DataFrame, types: List[Any]) -> bool:
        for fields in df.schema.fields:
            for type in types:
                if isinstance(fields.dataType, type):
                    return True
        return False

    def __flatten_arrays_and_structs(self, df: DataFrame) -> DataFrame:
        while self.__type_is_in_df(df, [ArrayType, StructType]):
            fields: List[Union[str, Column]] = []
            arrays: List[Column] = []
            for column in df.schema:
                if isinstance(column.dataType, ArrayType):
                    arrays.append(col(column.name))
                elif isinstance(column.dataType, StructType):
                    columns_in_struct = [
                        col(f"{column.name}.{new_column}").alias(
                            f"{column.name}-{new_column}"
                        )
                        for new_column in df.select(f"{column.name}.*").columns
                    ]
                    fields.extend(columns_in_struct)
                else:
                    fields.append(col(column.name))
            print("before")
            df.printSchema()
            if arrays:
                temp_column = "temp_exploded_column"
                df = df.select(*fields, explode(arrays_zip(*arrays)).alias(temp_column))
                df = df.select(*fields, f"{temp_column}.*")
                for array_column in arrays:
                    df = df.withColumnRenamed(
                        f"{temp_column}.{array_column}", str(array_column)
                    )
            else:
                df = df.select(*fields)
            print("after")
            df.printSchema()
        return df
