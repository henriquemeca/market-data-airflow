'''
File with custom operator for loading zacks Rank data from the zacks website
'''

from typing import Any, Iterable

from airflow.models import BaseOperator
from airflow.utils.context import Context
from helpers.zacks_rank_scrapper import ZacksTickerPage


class ZacksRankScrapper(BaseOperator):
    """Scrappes zacks rank data from a list of tickers"""

    def __init__(self, task_id: str, ticker_list_path: str) -> None:
        self.task_id = task_id
        self.ticker_list_path = ticker_list_path
        self.x = ZacksTickerPage
        super(BaseOperator, self).__init__()

    def execute(self, context: Context) -> Any:
        stock_ranks = zr.fetch_symbol_ranks(self._get_ticker_list_from_csv())

    def _get_ticker_list_from_csv(self) -> Iterable[str]:
        '''
        Get as ticker list from a file
        '''
        with open(self.ticker_list_path, mode="r", encoding='utf-8') as file:
            while True:
                line = file.readline()
                if not line:
                    break
                yield line.strip()

    async def fetch_symbol_ranks(symbols: Iterable[str]) -> dict[str, StockRank | Exception]:
        '''
        Fetchs zacks data of a list of tickers
        '''
        awaitables: list[Awaitable[tuple[str, StockRank | Exception]]] = []
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                awaitables.append(get_symbol_data(session, symbol))
            results = await asyncio.gather(*awaitables)
        return dict(results)
