import logging
from typing import List

import pandas as pd
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.utils.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    RAW_BUCKET,
    TICKER_PRICES_PATH,
)
from investidor_10.Operators.utils.json_response import (
    get_response_json,
    load_json_responses_to_gcs,
)


class HistoricalPrices:
    def __init__(self) -> None:
        self.url = "https://investidor10.com.br/api/cotacoes/acao/chart"
        self.url_sufix = "3650/true/real"  # 10 years reading
        self.raw_bucket = storage.Client().bucket(RAW_BUCKET)
        self.destination_folder = TICKER_PRICES_PATH

    def execute(self):
        tickers = self.__read_tickers_from_gcs()
        tickers_urls = {
            ticker: join_urls([self.url, ticker, self.url_sufix]) for ticker in tickers
        }
        logging.info("Tickers to be fetched: ", tickers_urls)
        ticker_prices = AsyncHTTPProcessor(
            id_url_dict=tickers_urls,
            headers=HEADER,
            response_processor=get_response_json,
        ).process()

        load_json_responses_to_gcs(
            responses=ticker_prices,
            destination_folder=self.destination_folder,
            bucket=self.raw_bucket,
        )

    def __read_tickers_from_gcs(self) -> List[str]:
        tickers_df = pd.read_csv(f"gs://{RAW_BUCKET}/{B3_TICKERS_ID_PATH}")
        valid_tickers = tickers_df[tickers_df["ticker_id"] != -1]
        return valid_tickers["ticker"].to_list()
