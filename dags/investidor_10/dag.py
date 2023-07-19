from datetime import datetime, timedelta
from typing import List

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)
from helpers.dataproc_operators import DataprocTasks
from helpers.read_yaml_config import read_yaml_config
from investidor_10.Operators.BFF_loader import BFFLoaderOperator
from investidor_10.Operators.tickers_tids import TickersIdsLoader
from investidor_10.Operators.utils.definitions import (
    BQ_TEMP_BUCKET,
    CLEANED_BUCKET,
    DAGS_ROOT_DIR,
    DATAPROC_GCP_REGION,
    DATAPROC_JARS_FOLDER,
    DATAPROC_WORK_DIR,
    GCP_PROJECT_ID,
    RAW_BUCKET,
)

config_yaml = read_yaml_config(f"{DAGS_ROOT_DIR}/investidor_10/config.yaml")
jars = [
    f"{DATAPROC_JARS_FOLDER}/gcs-connector-hadoop2-latest.jar",
    f"{DATAPROC_JARS_FOLDER}/spark-3.3-bigquery-0.31.1.jar",
]


with DAG(
    dag_id="investor_10_v1",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["MARKET"],
    max_active_tasks=4,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    bff_task_list: List[BFFLoaderOperator] = []
    cleaned_list: List[DataprocSubmitPySparkJobOperator] = []
    curated_list: List[DataprocSubmitPySparkJobOperator] = []
    data_proc = DataprocTasks(
        cluster_namme="investidor-10-cluster",
        region=DATAPROC_GCP_REGION,
        project_id=GCP_PROJECT_ID,
        jars=jars,
    )

    for config in config_yaml["bff-configs"]:
        bff_task = BFFLoaderOperator(
            task_id="raw_" + config["destination_folder"],
            url=config["url"],
            id=config["id"],
            url_sufix=config["url_sufix"],
            destination_folder=config["destination_folder"],
            retries=5,
            retry_delay=timedelta(minutes=2),
        )
        cleaning_task = data_proc.create_pyspark_task(
            "cleaned_" + config["destination_folder"],
            main_python_file_uri=f"gs://{DATAPROC_WORK_DIR}/{config['script']}",
            arguments=[
                f"--raw-path=gs://{RAW_BUCKET}/investidor_10/{config['destination_folder']}",
                f"--cleaned-path=gs://{CLEANED_BUCKET}/investidor_10/{config['destination_folder']}",
            ],
            pyfiles=[
                f"gs://{DATAPROC_WORK_DIR}/pyfiles/investidor10.zip",
                f"gs://{DATAPROC_WORK_DIR}/pyfiles/spark.zip",
            ],
        )
        curated_task = data_proc.create_pyspark_task(
            "trusted_" + config["destination_folder"],
            main_python_file_uri=f"gs://{DATAPROC_WORK_DIR}/scripts/investidor10/load_to_bq.py",
            arguments=[
                f"--cleaned-path=gs://{CLEANED_BUCKET}/investidor_10/{config['destination_folder']}",
                f"--table={config['destination_folder']}",
                f"--temporary-gcs-bucket={BQ_TEMP_BUCKET}",
            ],
            pyfiles=[
                f"gs://{DATAPROC_WORK_DIR}/pyfiles/investidor10.zip",
                f"gs://{DATAPROC_WORK_DIR}/pyfiles/spark.zip",
            ],
        )
        bff_task >> cleaning_task >> curated_task
        bff_task_list.append(bff_task)
        cleaned_list.append(cleaning_task)
        curated_list.append(curated_task)

    @task
    def load_tickers_ids():
        TickersIdsLoader().execute()

    load_tickers = load_tickers_ids()
    create_cluster = data_proc.create_cluster_task()

    start >> [load_tickers, create_cluster]
    load_tickers >> bff_task_list
    create_cluster >> cleaned_list
    curated_list >> data_proc.delete_cluster_task() >> end
