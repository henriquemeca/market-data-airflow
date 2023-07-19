from pyspark.sql import DataFrame


def load_on_bq(
    df: DataFrame, dataset: str, table: str, temporary_gcs_bucket: str
) -> None:
    "Load a given dataframe to the specified destination"
    bq_options = {
        "dataset": dataset,
        "table": table,
        "temporaryGcsBucket": temporary_gcs_bucket,
    }

    df.write.format("bigquery").options(**bq_options).mode("overwrite").save()
