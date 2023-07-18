from typing import List

from investidor10.clean_functions import clean_col_names, flatten_arrays_and_structs
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import DoubleType
from spark.argument_configuration.arg_config import ArgumentConfiguration
from spark.spark_client import spark_session


class PricesProfitCleaning:
    def __init__(self) -> None:
        self.config = ArgumentConfiguration(["raw_path", "cleaned_path"])
        self.raw_path = self.config.raw_path
        self.cleaned_path = self.config.cleaned_path

    def execute(self) -> None:
        with spark_session(f"cleaned_{self.raw_path}") as spark:
            df = spark.read.option("multiline", "true").json(f"{self.raw_path}/*.json")
            df = clean_col_names(df)
            df = flatten_arrays_and_structs(df)
            df = self.__unpivot_kpis_columns(df)
            df.write.mode("overwrite").parquet(self.cleaned_path)

    def __unpivot_kpis_columns(self, df: DataFrame) -> DataFrame:
        kpi_columns = df.columns
        kpi_columns.remove("ticker")
        kpi_columns = self.__generate_stacked_columns(kpi_columns)
        stack_str = f"stack({len(kpi_columns)}, {','.join(kpi_columns)}) AS (year, net_profit, quotation)"

        df = (
            df.select("ticker", expr(stack_str))
            .withColumn("net_profit", col("net_profit").cast(DoubleType()))
            .withColumn("quotation", col("quotation").cast(DoubleType()))
        )

        return df

    def __generate_stacked_columns(self, columns: List[str]) -> List[str]:
        years = sorted(set(column.split("-")[0] for column in columns))

        columns = []
        for year in years:
            columns.append(
                f"'{year}', cast(`{year}-net_profit` as string), cast(`{year}-quotation` as string)"
            )

        return columns


if __name__ == "__main__":
    PricesProfitCleaning().execute()
