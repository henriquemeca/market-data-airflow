'''
    File with custom operator for loading zacks Rank data from the zacks website
'''

import asyncio
import csv
import logging
from datetime import datetime
from typing import Any, Iterable

from airflow.models import BaseOperator
from airflow.utils.context import Context
from helpers.zacks_rank_scrapper import StockRank, fetch_symbol_ranks


class ZacksRankScrapper(BaseOperator):
    """Scrappes zacks rank data from a list of tickers"""

    def __init__(self, ticker_list_path: str, results_path: str,  limit: int = -1, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # type: ignore
        self.ticker_list_path = ticker_list_path
        self.results_path = results_path
        self.limit = limit

    def execute(self, context: Context) -> Any:
        asyncio.run(self.main())

    def get_ticker_list_from_csv(self, limit: int = -1) -> Iterable[str]:
        '''
            Yields a list of ticker from a csv file
        '''
        with open(self.ticker_list_path, "r", encoding='utf-8') as csvfile:
            ticker_list = csv.reader(csvfile, delimiter=',')
            for i, row in enumerate(ticker_list):
                if limit == i:
                    break
                yield row[0]

    async def main(self):
        '''
            Implements execute script from operator
        '''
        ticker_data = await fetch_symbol_ranks(self.get_ticker_list_from_csv(self.limit))
        today = datetime.today().strftime('%Y-%m-%d')
        with open(f"{self.results_path}/{today}_zacks_data.csv", "w", encoding='utf-8') as f:
            f.write("symbol\tzacks\tvalue\tgrowth\tmomtum\tvgm\tindustry\tdate\n")
            for (symbol, rank) in ticker_data.items():
                if isinstance(rank, StockRank):
                    f.write(
                        f"{symbol}\t{rank.zacks_rank}\t{rank.value}\t{rank.growth}\t{rank.momentum}\t{rank.vgm}\t{rank.industry}\t{today}\n")
                else:
                    f.write(f"{symbol}\t{rank!r}\n")
