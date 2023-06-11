import asyncio
import logging
import random
import re
from typing import Awaitable, Dict, List, Tuple

import aiohttp
import pandas as pd
from google.cloud import storage

from dags.investidor_10.Operators.helpers.definitions import HEADER


class BuildTickers:
    def __init__(self) -> None:
        # HTTP request variables
        self.tickers_url = "https://www.dadosdemercado.com.br/bolsa/acoes"
        self.url = "https://investidor10.com.br/acoes/"

        self.ticker_regex = re.compile(
            r"refreshComponent\('rating-component'.*?'id':\s(\d+)", re.DOTALL
        )
        self.bucket_name = "raw_data_layer"
        self.destination_file = "investidor_10/tickers_ids/b3_tickers.csv"

    async def __get_ticker_id(
        self, session: aiohttp.ClientSession, ticker: str
    ) -> Tuple[str, int]:
        url = self.url + ticker.lower()

        # Sleeping to not overload server
        await asyncio.sleep(random.random() * 100)
        async with session.get(url, headers=HEADER) as response:
            response_txt = await response.text()
            ticker_regex_match = self.ticker_regex.search(response_txt)
            if ticker_regex_match:
                ticker_id = int(ticker_regex_match.group(1))
                logging.info(f"{ticker}:{ticker_id}")
                return (ticker, ticker_id)
            logging.info(f"No id for {ticker}")
            return (ticker, -1)

    async def __gather_ticker_ids(self, ticker_list: List[str]) -> Dict[str, int]:
        awaitables: list[Awaitable[tuple[str, int]]] = []
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(100 * 60)
        ) as session:
            for ticker in ticker_list:
                awaitables.append(self.__get_ticker_id(session, ticker))
            results = await asyncio.gather(*awaitables)
        return dict(results)

    def __save_ticker_ids_to_csv(
        self, client: storage.Client, tickers: Dict[str, int]
    ) -> None:
        csv_data = r"ticker, value\n" + "\n".join(
            f"{ticker},{value}" for ticker, value in tickers.items()
        )
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.destination_file)
        blob.upload_from_string(csv_data, content_type="text/csv")

    async def download_ticker_ids(self, client: storage.Client) -> None:
        b3_tickers = pd.read_html(self.tickers_url)[0]["Código"].tolist()
        tickers = await self.__gather_ticker_ids(b3_tickers)
        self.__save_ticker_ids_to_csv(client, tickers)

    def __file_exists(self, client: storage.Client) -> bool:
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.destination_file)
        return blob.exists()

    def load_b3_tickers_ids(self):
        client = storage.Client()
        if not self.__file_exists(client):
            logging.info(
                f"Tickers were not found in {self.bucket_name}/{self.destination_file}. Downloading ticker reference"
            )
            asyncio.run(self.download_ticker_ids(client))
        else:
            logging.info(
                f"Tickers were already loaded in {self.bucket_name}/{self.destination_file}."
            )
