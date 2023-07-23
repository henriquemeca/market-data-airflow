from investidor10.clean_functions import clean_col_names, flatten_arrays_and_structs
from spark.argument_configuration.arg_config import ArgumentConfiguration
from spark.spark_client import spark_session


class HistoricKpisCleaning:
    """Class for processin investidor_10 historic_kpis data"""

    def __init__(self) -> None:
        self.config = ArgumentConfiguration(["raw_path", "cleaned_path"])
        self.raw_path = self.config.raw_path
        self.cleaned_path = self.config.cleaned_path

    def execute(self) -> None:
        with spark_session(f"cleaned_{self.raw_path}") as spark:
            df = spark.read.option("multiline", "true").json(f"{self.raw_path}/*.json")
            df = df.select("ticker", "data.*")
            df = clean_col_names(df)
            df = flatten_arrays_and_structs(df)
            df = clean_col_names(df)
            df.write.mode("overwrite").parquet(self.cleaned_path)


if __name__ == "__main__":
    HistoricKpisCleaning().execute()
