import os
from datetime import datetime, timedelta
from typing import List

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from helpers.read_yaml_config import read_yaml_config
from investidor_10.Operators.BFF_loader import BFFLoaderOperator
from investidor_10.Operators.tickers_tids import TickersIdsLoader

DAGS_ROOT_DIR = os.getenv("DAGS_ROOT_DIR")

config_yaml = read_yaml_config(f"{DAGS_ROOT_DIR}/investidor_10/config.yaml")


with DAG(
    dag_id="investor_10_v1",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["MARKET"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    bff_task_list: List[BFFLoaderOperator] = []
    for config in config_yaml["bff_configs"]:
        bff_task_list.append(
            BFFLoaderOperator(
                task_id=config["task_id"],
                url=config["url"],
                id=config["id"],
                url_sufix=config["url_sufix"],
                destination_folder=config["destination_folder"],
                retries=5,
                retry_delay=timedelta(minutes=2),
            )
        )

    @task
    def load_tickers_ids():
        TickersIdsLoader().execute()

    start >> load_tickers_ids() >> bff_task_list >> end
