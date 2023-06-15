'''
    Creates a dag that reads a csv file with zacks rank data and inserts into Bigquery
'''
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.decorators import task
from airflow.models import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Dag default values
SCHEDULE_INTERVAL = None
START_DATE = datetime(2022, 11, 13)
CATCHUP = False

# Task parameters
PROJECT_ID = 'henrique-projects'
DATASET_ID = 'zacks'
TABLE_ID = 'zacks_rank_scraping'


def insert_rows(data: list[dict[str, Any]]):
    '''
        Insert data into BigQuery
    '''
    BigQueryHook().insert_all(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        rows=data,
        ignore_unknown_values=False,
        skip_invalid_rows=False,
        fail_on_error=True,
    )


def _value(value: Any):
    '''
        If value is NaN return None
    '''
    return value if value == value else None


with DAG(
    dag_id='zacks_rank_batch',
    description='Loads ticker data from a zacks website to specified destination',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
) as dag:

    @task()
    def load_zacks_data_to_bq():
        '''
            Reads a csv file, parses and insert the values into BigQuery
        '''
        zacks_data = pd.read_csv(
            './plugins/data/zacks_rank/export_data.csv')
        zacks_data_list = []
        for (index, row) in zacks_data.iterrows():
            data = {
                "ticker": row['Symbol'],
                "rank": _value(row['ZacksRank']),
                "value_score": _value(row['ValueScore']),
                "growth_score": _value(row['GrowthScore']),
                "momentum_score": _value(row['MomentumScore']),
                "vgm_score": _value(row['VGMScore']),
                "industry": _value(row['Industry']),
                "reference_date": row['reference_date']
            }
            zacks_data_list.append(data)
            if len(zacks_data_list) == 10000:
                print('inserting data')
                insert_rows(zacks_data_list)
                zacks_data_list = []

        if bool(zacks_data_list):
            insert_rows(zacks_data_list)

    load_zacks_data_to_bq()
