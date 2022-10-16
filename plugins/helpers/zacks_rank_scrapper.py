'''
Downlaods zacks ranks data using webscrapping
'''
import asyncio
import random
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction
from html.parser import HTMLParser
from typing import Any, Awaitable, NamedTuple, Tuple

import aiohttp


class ZacksTickerPage(HTMLParser):
    """
    Class used to acess the ticker page from zacks website and scrap and save rank information
    """

    def __init__(self):
        super().__init__()
        self.scope_depth: int = 0
        self.data: list[str] = []

    def error(self, message: str) -> Any:
        '''Part of the HTLMParser ABC'''

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "p" and ("class", "rank_view") in attrs and self.scope_depth == 0:
            self.scope_depth = 1
        elif self.scope_depth > 0:
            self.scope_depth += 1

    def handle_data(self, data: str) -> None:
        if self.scope_depth > 0:
            self.data.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self.scope_depth > 0:
            self.scope_depth -= 1
            if self.scope_depth == 0:
                self.data.append("\n")

    def result(self) -> list[str]:
        """Format a document into a list of line"""
        lines: list[str] = []
        for line in "".join(self.data).split("\n"):
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return lines


@dataclass
class Rank(Enum):
    """All zacs ranks options"""
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'
    E = 'E'
    F = 'F'


@dataclass
class StockRank(NamedTuple):
    """Ranks information about a stock"""
    zacks_rank: int
    value: Rank
    growth: Rank
    momentum: Rank
    vgm: Rank
    industry: str
    industry_rank: str


def extract_data(body: str) -> StockRank:
    """
    Receives the body of a zacks rank page and returns a StockRank
    dataclass with correpondent data"""
    parser = ZacksTickerPage()
    parser.feed(body)
    lines = parser.result()
    if len(lines) < 4:
        raise RuntimeError("Too little lines parsed")

    zacks_match = re.search("[0-9]", lines[0])
    if zacks_match is None:
        raise RuntimeError("Zacks rank was not found")
    zacks_rank = int(zacks_match[0])

    subrank: dict[str, Rank] = {}
    for key in ["value", "growth", "momentum", "vgm"]:
        match = re.search("([A-F])\xa0" + key, lines[1], re.I)
        if match is None:
            raise RuntimeError(f"Subrank {key} was not found")
        subrank[key] = Rank(match[1])

    industry_match = re.search("Industry: (.*)", lines[3], re.I)
    industry = lines[3] if industry_match is None else industry_match[1]

    return StockRank(zacks_rank, industry_rank=lines[2], industry=industry, **subrank)


async def get_symbol_data(
        session: aiohttp.ClientSession,
        symbol: str) -> Tuple[str, StockRank | Exception]:
    '''Receives a ticker as a input and returns StockRank data'''
    try:
        url = f"https://www.zacks.com/stock/quote/{symbol}"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
        }
        # Slow, but nice to zack's servers
        await asyncio.sleep(random.random() * 50)
        async with session.get(url, headers=headers) as response:
            print(f"Status for {symbol}: {response.status}")
            extracted = extract_data(await response.text())
            return (symbol, extracted)
    # this error will be saved in the output
    except Exception as error:  # pylint: disable=W0703
        return (symbol, error)


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


async def main() -> None:
    '''
    Implements the scrapping process for loading ticker zacks data
    '''
    # Fração para testar em subconjuntos de símbolos
    # export PYTHONHASHSEED=algum número fixa o subconjunto
    stock_ranks = await fetch_symbol_ranks(generate_symbols("lines.txt", Fraction(1, 100)))
    with open("result.csv", mode="w", encoding='utf-8') as f:
        f.write("sym\tzacks\tvalue\tgrowth\tmomtum\tvgm\tindustry\n")
        for (symbol, rank) in stock_ranks.items():
            if isinstance(rank, StockRank):
                f.write(
                    (f"{symbol}\t{rank.zacks_rank}\t{rank.value}\t{rank.growth}\t"
                        + f"{rank.momentum}\t{rank.vgm}\t{rank.industry}\n"))
            else:
                f.write(f"{symbol}\t{rank!r}\n")

if __name__ == "__main__":
    asyncio.run(main())
    # with open("test.html", "r") as f:
    #    print(extract_data(f.read()))

# vim: ts=4:sw=4:et
