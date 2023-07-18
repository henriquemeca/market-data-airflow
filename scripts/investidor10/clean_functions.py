from typing import Any, List, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import arrays_zip, col, explode
from pyspark.sql.types import ArrayType, StructType


def __type_is_in_df(df: DataFrame, types: List[Any]) -> bool:
    for fields in df.schema.fields:
        for type in types:
            if isinstance(fields.dataType, type):
                return True
    return False


def flatten_arrays_and_structs(df: DataFrame) -> DataFrame:
    while __type_is_in_df(df, [ArrayType, StructType]):
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
        print(fields)
        print(arrays)
        print("before")
        df.printSchema()
        if arrays:
            temp_column = "temp_exploded_column"
            df = df.select(
                *fields, explode(arrays_zip(*arrays)).alias(temp_column)
            ).select(*fields, f"{temp_column}.*")
            for array_column in arrays:
                df = df.withColumnRenamed(
                    f"{temp_column}.{array_column}", str(array_column)
                )
        else:
            df = df.select(*fields)
        print("after")
        df.printSchema()
    return df


def clean_col_names(df: DataFrame) -> DataFrame:
    df = df.select("ticker", "data.*")
    renamed_cols = [
        col_name.replace("Í", "i")
        .replace("Ô", "o")
        .replace(" / ", "_")
        .replace(" (", "_")
        .replace(")", "")
        .replace("/", "_")
        .replace(" ", "_")
        .replace(".", "_")
        .lower()
        for col_name in df.columns
    ]
    for inital_column, renamed_column in zip(df.columns, renamed_cols):
        print(inital_column, renamed_column)
        df = df.withColumnRenamed(inital_column, renamed_column)
    return df


def remove_substring_from_col_names(df: DataFrame, substring: str) -> DataFrame:
    for column in df.columns:
        if substring in column:
            df = df.withColumnRenamed(column, column.replace(substring, ""))

    return df
