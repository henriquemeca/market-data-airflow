from investidor10.clean_functions import (
    flatten_arrays_and_structs,
    remove_substring_from_col_names,
)
from spark.argument_configuration.arg_config import ArgumentConfiguration
from spark.spark_client import spark_session


class DefaultCleaning:
    def __init__(self) -> None:
        self.config = ArgumentConfiguration(["raw_path", "cleaned_path"])
        self.raw_path = self.config.raw_path
        self.cleaned_path = self.config.cleaned_path

    def execute(self) -> None:
        with spark_session(f"cleaned_{self.raw_path}") as spark:
            df = spark.read.option("multiline", "true").json(f"{self.raw_path}/*.json")

            df = flatten_arrays_and_structs(df)
            df = remove_substring_from_col_names(df, "data-")
            df.write.mode("overwrite").parquet(self.cleaned_path)


if __name__ == "__main__":
    DefaultCleaning().execute()
