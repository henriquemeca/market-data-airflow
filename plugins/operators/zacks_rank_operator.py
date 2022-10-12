'''
File with custom operator for loading zacks Rank data from the zacks website
'''
# docstring

from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context

class ZacksRankScrapper(BaseOperator):
    """Scrappes zacks rank data from a list of tickers"""
    def __init__(self, task_id) -> None:
        self.task_id = task_id
        super(BaseOperator,self).__init__()
    def execute(self, context: Context) -> Any:
        pass
