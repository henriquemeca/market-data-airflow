'''
    File with custom operator for loading zacks Rank data from the zacks website
'''

import asyncio
import csv
import logging
from datetime import datetime
from typing import Any, Iterable

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.context import Context
from helpers.zacks_rank_scrapper import StockRank, fetch_symbol_ranks


class ZacksRankScrapper(BaseOperator):
    """Scrappes zacks rank data from a list of tickers"""

    def __init__(self,
                 ticker_list_path: str,
                 project_id: str,
                 dataset_id: str,
                 table_id: str,
                 export_results: bool = False,
                 results_path: str = '',
                 limit: int = -1,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)  # type: ignore
        self.ticker_list_path = ticker_list_path
        self.export_results = export_results
        self.results_path = results_path
        self.limit = limit
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def execute(self, context: Context) -> Any:
        asyncio.run(self.main())

    async def main(self):
        '''
            Implements execute script from operator
        '''
        ticker_data = await fetch_symbol_ranks(get_ticker_list_from_csv(self.ticker_list_path, self.limit))
        today = datetime.today().strftime('%Y-%m-%d')
        ticker_list = []
        for (ticker, rank) in ticker_data.items():
            if isinstance(rank, StockRank):
                parsed_dict = {
                    "ticker": ticker,
                    "rank":	rank.zacks_rank,
                    "value_score": _to_string(rank.value),
                    "growth_score": _to_string(rank.growth),
                    "momentum_score": _to_string(rank.momentum),
                    "vgm_score": _to_string(rank.vgm),
                    "industry": rank.industry,
                    "reference_date": today,
                    "industry_rank_range": rank.industry_rank
                }
                ticker_list.append(parsed_dict)
            else:
                continue
        print(self.project_id)
        print(self.dataset_id)
        print(self.table_id)
        BigQueryHook().insert_all(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            rows=ticker_list,
            ignore_unknown_values=False,
            skip_invalid_rows=False,
            fail_on_error=True,
        )
        if self.export_results:
            with open(f"{self.results_path}/{today}_zacks_data.csv", "w", encoding='utf-8') as f:
                f.write(
                    "symbol\tzacks\tvalue\tgrowth\tmomtum\tvgm\tindustry\tdate\n")
                for (symbol, rank) in ticker_data.items():
                    if isinstance(rank, StockRank):
                        f.write(
                            f"{symbol}\t{rank.zacks_rank}\t{rank.value}\t{rank.growth}\t{rank.momentum}\t{rank.vgm}\t{rank.industry}\t{today}\n")
                    else:
                        f.write(f"{symbol}\t{rank!r}\n")


def _to_string(data: Any):
    '''
        Converts objects to string if they are not None
    '''
    return str(data) if data is not None else data


def get_ticker_list_from_csv(ticker_list_path: str, limit: int = -1) -> Iterable[str]:
    '''
        Yields a list of ticker from a csv file
    '''
    with open(ticker_list_path, "r", encoding='utf-8') as csvfile:
        ticker_list = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(ticker_list):
            if limit == i:
                break
            yield row[0]
