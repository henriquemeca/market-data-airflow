import asyncio
import logging
import random
from typing import Any, Awaitable, Dict, List, Tuple

import aiohttp
import pandas as pd
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.helpers.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    RAW_BUCKET,
    TICKER_PRICES_RAW_PATH,
)


class HistoricalPrices:
    def __init__(self) -> None:
        self.url = "https://investidor10.com.br/api/cotacoes/acao/chart"
        self.url_sufix = "3650/true/real"  # 10 years reading
        self.raw_bucket = storage.Client().bucket(RAW_BUCKET)
        self.destination_folder = TICKER_PRICES_RAW_PATH

    def execute(self):
        tickers = self.__read_tickers_from_gcs()
        tickers_urls = {
            ticker: join_urls([self.url, ticker, self.url_sufix]) for ticker in tickers
        }
        logging.info("Tickers to be fetched: ", tickers_urls)
        ticker_prices = AsyncHTTPProcessor(
            id_url_dict=tickers_urls,
            headers=HEADER,
            response_processor=self.__get_ticker_prices_json,
        ).process()

        for ticker, prices in ticker_prices.items():
            destination_file = join_urls([self.destination_folder, f"{ticker}.json"])
            blob = self.raw_bucket.blob(destination_file)
            blob.upload_from_string(prices, content_type="application/json")
            logging.info(f"{destination_file} was loaded.")

    def __read_tickers_from_gcs(self) -> List[str]:
        tickers_df = pd.read_csv(f"gs://{RAW_BUCKET}/{B3_TICKERS_ID_PATH}")
        valid_tickers = tickers_df[tickers_df["ticker_id"] != -1]
        return valid_tickers["ticker"].to_list()

    async def __get_ticker_prices_json(
        self, id: str, response: aiohttp.ClientResponse
    ) -> Tuple[str, str]:
        return (id, await response.text())
