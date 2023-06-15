import logging
import random
from typing import Dict, Tuple

import aiohttp
import pandas as pd
from airflow.models import BaseOperator
from google.cloud import storage
from helpers.http_client.async_http_processor import AsyncHTTPProcessor
from helpers.join_url import join_urls
from investidor_10.Operators.utils.definitions import (
    B3_TICKERS_ID_PATH,
    HEADER,
    RAW_BUCKET,
)


class BFFLoaderOperator(BaseOperator):
    """
    This operator loads data from investidor 10 BFFs (Backend for Frontend) and stores it in a GCS bucket.
    It fetches data based on provided URLs, processes the response, and saves the obtained content in the destination folder.

    :param url: The base endpoint to be loaded.
    :param id: A column to be read from the b3_tickers file. There are 3 options: ticker, ticker_id, and company_id.
    :param destination_folder: The folder where the output files will be saved.
    :param url_sufix: An optional suffix to be appended to the base URL. Default is an empty string.
    :param timeout_seconds: The timeout for the async session in seconds. Default value is 10 minutes (600 seconds).
    :param sleep_seconds: The sleep time for the async process in seconds. Default value is a random number between 0 and 100.
    """

    def __init__(
        self,
        url,
        id: str,
        destination_folder: str,
        url_sufix="",
        timeout_seconds=10 * 60,
        sleep_seconds=random.random() * 100,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.url = url
        self.url_sufix = url_sufix
        self.id = id
        self.raw_bucket = storage.Client().bucket(RAW_BUCKET)
        self.destination_folder = destination_folder
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds

    def execute(self):
        tickers = self.__read_tickers_from_gcs()
        tickers_urls = {
            ticker: join_urls([self.url, str(id), self.url_sufix])
            for ticker, id in tickers.items()
        }
        logging.info("Tickers to be fetched: ", tickers_urls)
        tickers_income = AsyncHTTPProcessor(
            id_url_dict=tickers_urls,
            headers=HEADER,
            response_processor=self.__get_response_json,
            timeout_seconds=self.timeout_seconds,
            sleep_seconds=self.sleep_seconds,
        ).process()

        self.__load_json_responses_to_gcs(
            responses=tickers_income,
            destination_folder=self.destination_folder,
            bucket=self.raw_bucket,
        )

    def __read_tickers_from_gcs(self) -> Dict[str, str]:
        """
        Returns a Dictionary like 'ticker:id', where the id can any of the columns
        from the b3_tickers.csv
        This file contains a list of all available ticker and the ids realated to them
        in the investidor 10 website.
        """
        tickers_df = pd.read_csv(f"gs://{RAW_BUCKET}/{B3_TICKERS_ID_PATH}")
        valid_tickers = tickers_df[tickers_df["ticker_id"] != -1]
        return dict(zip(valid_tickers["ticker"], valid_tickers[self.id.value]))

    async def __get_response_json(
        self, id: str, response: aiohttp.ClientResponse
    ) -> Tuple[str, str]:
        return (id, await response.text())

    def __load_json_responses_to_gcs(
        self, responses: Dict[str, str], destination_folder: str, bucket: storage.Bucket
    ) -> None:
        for ticker, json_data in responses.items():
            destination_file = join_urls([destination_folder, f"{ticker}.json"])
            blob = bucket.blob(destination_file)
            blob.upload_from_string(json_data, content_type="application/json")
            logging.info(f"{destination_file} was loaded.")
