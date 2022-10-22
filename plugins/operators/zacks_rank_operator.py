'''
File with custom operator for loading zacks Rank data from the zacks website
'''

import asyncio
import logging
from typing import Any, Awaitable, Dict, Iterable, Tuple

from airflow.models import BaseOperator
from airflow.utils.context import Context
from helpers.zacks_rank_scrapper import get_stock_rank


class ZacksRankScrapper(BaseOperator):
    """Scrappes zacks rank data from a list of tickers"""

    def __init__(self, ticker_list_path: str, limit: int = -1, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ticker_list_path = ticker_list_path
        self.limit = limit

    def execute(self, context: Context) -> Any:
        asyncio.run(main(self.get_ticker_list_from_csv(self.limit)))
