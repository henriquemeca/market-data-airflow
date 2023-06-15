'''
    Creates a dag for scraping data daily and inserting into Bigquery
'''
from datetime import datetime

from airflow.models import DAG
from operators.zacks_rank_operator import ZacksRankScrapper

# Dag  default values
SCHEDULE_INTERVAL = "5 0 * * *"
START_DATE = datetime(2022, 10, 23)
CATCHUP = False

# Task parametes
TICKER_LIST_PATH = "plugins/data/zacks_rank/choosenTickers.csv"
PROJECT_ID = 'henrique-projects'
DATASET_ID = 'zacks'
TABLE_ID = 'zacks_rank_scraping'
LIMIT: int = -1  # Limiting the number of tickers to be loaded from the list


with DAG(
    dag_id='zacks_rank',
    description='Loads ticker data from a zacks website to specified destination',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    doc_md='''
        This dag was designed to receive read a list of tickers and scrap the corresponding zacks rank
        data from https://www.zacks.com/ and load them in a specified path
    ''',
) as dag:

    zacks_rank = ZacksRankScrapper(
        task_id='zacks_rank_scrapper',
        ticker_list_path=TICKER_LIST_PATH,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        # results_path='plugins/data/zacks_rank/results',
        limit=LIMIT
    )

    zacks_rank
