import logging
import re
from typing import Any, Dict, List, Tuple

import aiohttp
import pandas as pd
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.helpers.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    RAW_BUCKET,
)


class BuildTickers:
    def __init__(self) -> None:
        self.tickers_url = "https://www.dadosdemercado.com.br/bolsa/acoes"
        self.url = "https://investidor10.com.br/acoes"

        self.ticker_regex = re.compile(
            r"refreshComponent\('rating-component'.*?'id':\s(\d+)", re.DOTALL
        )
        self.company_regex = re.compile(r"/api/balancos/receitaliquida/chart/(\d+)/")

    def execute(self):
        client = storage.Client()
        bucket = client.bucket(RAW_BUCKET)
        if not bucket.blob(B3_TICKERS_ID_PATH).exists():
            logging.info(
                f"Tickers were not found in {RAW_BUCKET}/{B3_TICKERS_ID_PATH}. Downloading ticker reference"
            )
            self.__download_ticker_ids(bucket)
        else:
            logging.info(
                f"Tickers were already loaded in {RAW_BUCKET}/{B3_TICKERS_ID_PATH}."
            )

    def __download_ticker_ids(self, bucket: storage.Bucket) -> None:
        tickers_urls = self.__get_ticker_urls()
        tickers: Dict[str, Dict] = AsyncHTTPProcessor(
            id_url_dict=tickers_urls,
            headers=HEADER,
            response_processor=self.__process_requested_ticker,
        ).process()
        self.__save_ticker_ids_to_csv(bucket, tickers)

    def __get_ticker_urls(self) -> Dict[str, str]:
        b3_tickers: List[str] = pd.read_html(self.tickers_url)[0]["Código"].tolist()
        return {
            ticker.lower(): join_urls([self.url, ticker.lower()])
            for ticker in b3_tickers
        }

    async def __process_requested_ticker(
        self, ticker: str, response: aiohttp.ClientResponse
    ) -> Tuple[str, Dict[str, Any]]:
        response_txt = await response.text()

        ticker_regex_match = self.ticker_regex.search(response_txt)
        if ticker_regex_match:
            ticker_id = int(ticker_regex_match.group(1))
        else:
            ticker_id = -1

        company_regex_match = self.company_regex.search(response_txt)
        if company_regex_match:
            company_id = int(company_regex_match.group(1))
        else:
            company_id = -1

        logging.info(f"{ticker}->ticker_id:{ticker_id}|company_id:{company_id}")
        return (ticker, {"ticker_id": ticker_id, "company_id": company_id})

    def __save_ticker_ids_to_csv(
        self, bucket: storage.Bucket, tickers: Dict[str, Dict]
    ) -> None:
        csv_columns = "ticker,ticker_id,company_id\n"
        ticker_data = [
            f"{ticker},{data['ticker_id']},{data['company_id']}"
            for ticker, data in tickers.items()
        ]
        csv_data = csv_columns + "\n".join(ticker_data)
        blob = bucket.blob(B3_TICKERS_ID_PATH)
        blob.upload_from_string(csv_data, content_type="text/csv")
