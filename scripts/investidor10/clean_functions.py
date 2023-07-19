from typing import Any, Dict, List, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import arrays_zip, col, explode
from pyspark.sql.types import ArrayType, StructType


def __type_is_in_df(df: DataFrame, types: List[Any]) -> bool:
    "Check if a given pyspark type is present in the Dataframe schema fields"
    for fields in df.schema.fields:
        for type in types:
            if isinstance(fields.dataType, type):
                return True
    return False


def flatten_arrays_and_structs(df: DataFrame) -> DataFrame:
    """
    Receives a dataframe with columns of types Array and Struct and flattens the dataframe.

    There is a loop that iterates over the dataframe until there's no more any Arrays or Structs,
    even considering that these types can be nested inside the column

    Its important to consider that arrays over many columns should have the same amount of elements.
    When a Struct type is breaked into many columns, the parente column name is included in the final column name
    """
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
    return df


def __replace_invalid_characters(target_string: str) -> str:
    "Replaces invalid characters from a give string"
    invalid_to_valid_characters = {
        "Í": "i",
        "Ô": "o",
        " / ": "_",
        " (": "_",
        ")": "",
        "/": "_",
        " ": "_",
        ".": "_",
    }
    for invalid_char, valid_char in invalid_to_valid_characters.items():
        target_string = target_string.replace(invalid_char, valid_char)
    return target_string.lower()


def clean_col_names(df: DataFrame) -> DataFrame:
    "Rename the columns of a dataframe replacing non allowed characters"
    renamed_cols = [__replace_invalid_characters(col_name) for col_name in df.columns]
    for inital_column, renamed_column in zip(df.columns, renamed_cols):
        df = df.withColumnRenamed(inital_column, renamed_column)
    return df


def remove_substring_from_col_names(df: DataFrame, substring: str) -> DataFrame:
    """
    This function will iterate over the columns of the Dataframe and remove a given substring.
    Ex:
    substring = "data-"
    column_before = "data-column_name"
    column_after = "column_name
    """
    for column in df.columns:
        if substring in column:
            df = df.withColumnRenamed(column, column.replace(substring, ""))
    return df
