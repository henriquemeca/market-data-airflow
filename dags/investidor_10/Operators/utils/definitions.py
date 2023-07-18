import os

from airflow.models.variable import Variable

# HTTP Requests variables
HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

# GCS vars
RAW_BUCKET = Variable.get("raw-layer")
CLEANED_BUCKET = Variable.get("cleaned-layer")

# Project vars
GCP_PROJECT_ID = Variable.get("gcp-project-id")
BQ_TEMP_BUCKET = os.getenv("BQ_TEMP_BUCKET")
DAGS_ROOT_DIR = os.getenv("DAGS_ROOT_DIR")

# Dataproc vars
DATAPROC_GCP_REGION = Variable.get("dataproc-gcp-region")
DATAPROC_WORK_DIR = Variable.get("dataproc-work-dir")
DATAPROC_JARS_FOLDER = f"gs://{Variable.get('dataproc-work-dir')}/jars"

# Investidor10 vars
TICKER_PRICES_PATH = "investidor_10/ticker_prices"
NET_INCOME_PATH = "investidor_10/net_income"
B3_TICKERS_ID_PATH = "investidor_10/tickers_ids/b3_tickers.csv"
