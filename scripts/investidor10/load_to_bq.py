from spark.argument_configuration.arg_config import ArgumentConfiguration
from spark.load_on_bq import load_on_bq
from spark.spark_client import spark_session


class LoadOnBQ:
    """Class for processin investidor_10 historic_kpis data"""

    def __init__(self) -> None:
        self.config = ArgumentConfiguration(
            ["cleaned_path", "table", "temporary_gcs_bucket"]
        )
        self.cleaned_path = self.config.cleaned_path
        self.dataset = "investidor_10"
        self.table = self.config.table
        self.temporary_gcs_bucket = self.config.temporary_gcs_bucket

    def execute(self) -> None:
        with spark_session(f"trusted_{self.cleaned_path}") as spark:
            df = spark.read.parquet(f"{self.cleaned_path}")
            load_on_bq(
                df=df,
                dataset=self.dataset,
                table=self.table,
                temporary_gcs_bucket=self.temporary_gcs_bucket,
            )


if __name__ == "__main__":
    LoadOnBQ().execute()
