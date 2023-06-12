import logging
import random
from typing import Dict, Tuple

import aiohttp
import pandas as pd
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.utils.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    NET_INCOME_PATH,
    RAW_BUCKET,
)
from investidor_10.Operators.utils.json_response import load_json_responses_to_gcs


class NetIncome:
    def __init__(self) -> None:
        self.url = "https://investidor10.com.br/api/balancos/receitaliquida/chart/"
        self.url_sufix = "/3650/0/"  # 10 years reading
        self.raw_bucket = storage.Client().bucket(RAW_BUCKET)
        self.destination_folder = NET_INCOME_PATH

    def execute(self):
        tickers = self.__read_tickers_from_gcs()
        tickers_urls = {
            ticker: join_urls([self.url, str(company_id), self.url_sufix])
            for ticker, company_id in tickers.items()
        }
        logging.info("Tickers to be fetched: ", tickers_urls)
        tickers_income = AsyncHTTPProcessor(
            id_url_dict=tickers_urls,
            headers=HEADER,
            response_processor=self.__get_ticker_prices_json,
            timeout_seconds=100 * 60,
            sleep_seconds=random.random() * 100,
        ).process()

        load_json_responses_to_gcs(
            responses=tickers_income,
            destination_folder=self.destination_folder,
            bucket=self.raw_bucket,
        )

    def __read_tickers_from_gcs(self) -> Dict[str, str]:
        tickers_df = pd.read_csv(f"gs://{RAW_BUCKET}/{B3_TICKERS_ID_PATH}")
        valid_tickers = tickers_df[tickers_df["ticker_id"] != -1]
        return dict(zip(valid_tickers["ticker"], valid_tickers["company_id"]))

    async def __get_ticker_prices_json(
        self, id: str, response: aiohttp.ClientResponse
    ) -> Tuple[str, str]:
        return (id, await response.text())
