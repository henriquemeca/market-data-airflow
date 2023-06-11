"""
Downlaods zacks ranks data using webscrapping
"""
import asyncio
import random
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction
from html.parser import HTMLParser
from typing import Any, Awaitable, List, NamedTuple, Tuple, Union

import aiohttp


class ZacksTickerPage(HTMLParser):
    """
    Class used to acess the ticker page from zacks website and scrap and save rank information
    """

    def __init__(self):
        super().__init__()
        self.scope_depth: int = 0
        self.data: List[str] = []

    def error(self, message: str) -> Any:
        """Part of the HTLMParser ABC"""

    def handle_starttag(
        self, tag: str, attrs: List[Tuple[str, Union[str, None]]]
    ) -> None:
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
        lines: List[str] = []
        for line in "".join(self.data).split("\n"):
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return lines


@dataclass
class Rank(Enum):
    """All zacs ranks options"""

    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"

    def __str__(self):
        return self.value


class StockRank(NamedTuple):
    """Ranks information about a stock"""

    zacks_rank: int | None
    value: Rank | None
    growth: Rank | None
    momentum: Rank | None
    vgm: Rank | None
    industry: str | None
    industry_rank: str | None


def extract_data(body: str, symbol: str) -> StockRank:
    """
    Parses the data from zacks html into a StockRank object
    """
    parser = ZacksTickerPage()
    parser.feed(body)
    lines = parser.result()

    try:
        str_pattern = lines[0]
    except IndexError:
        return StockRank(*tuple([None] * 7))

    zacks_match = re.search("[0-9]", str_pattern)
    if zacks_match is None:
        zacks_rank = None
    else:
        zacks_rank = int(zacks_match[0])

    subrank: dict[str, Union[Rank, None]] = {}
    for key in ["value", "growth", "momentum", "vgm"]:
        match = re.search("([A-F])\xa0" + key, lines[1], re.I)
        if match is None:
            subrank[key] = None
        else:
            subrank[key] = Rank(match[1])  # type: ignore
    try:
        industry_match = re.search("Industry: (.*)", lines[3], re.I)
        industry = lines[3] if industry_match is None else industry_match[1]
    except IndexError:
        industry = None
    return StockRank(zacks_rank, industry_rank=lines[2], industry=industry, **subrank)


async def get_symbol_data(
    session: aiohttp.ClientSession, symbol: str
) -> Tuple[str, StockRank | Exception]:
    """
    Loads html data from the zacks website
    """
    try:
        url = f"https://www.zacks.com/stock/quote/{symbol}"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
        }
        # Seja legal com o servidor não sendo tão eficiente
        await asyncio.sleep(random.random() * 5)
        async with session.get(url, headers=headers) as response:
            print(f"Status for {symbol}: {response.status}")
            extracted = extract_data(await response.text(), symbol)
            return (symbol, extracted)
    except Exception as e:
        return (symbol, e)


async def fetch_symbol_ranks(
    symbols: Iterable[str],
) -> dict[str, StockRank | Exception]:
    """
    Gets a list of tickers and returns their data, loaded async
    """
    awaitables: list[Awaitable[tuple[str, StockRank | Exception]]] = []
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(100 * 60)
    ) as session:
        for symbol in symbols:
            awaitables.append(get_symbol_data(session, symbol))
        results = await asyncio.gather(*awaitables)
    return dict(results)


def generate_symbols(filename: str, ratio: Fraction = Fraction(1, 1)) -> Iterable[str]:
    """
    Reads a file to generate a list of tickers
    """
    with open(filename, "r", encoding="utf-8") as file:
        while True:
            line = file.readline()
            if not line:
                break
            sym = line.strip()
            if hash(sym) % ratio.denominator < ratio.numerator:
                yield line.strip()


async def main() -> None:
    """
    Executes a script to test the scraper function
    """
    stock_ranks = await fetch_symbol_ranks(
        generate_symbols(
            r".\plugins\data\zacks_rank\choosenTickers.csv", Fraction(1, 500)
        )
    )
    with open("result.csv", "w", encoding="utf-8") as f:
        f.write("sym\tzacks\tvalue\tgrowth\tmomtum\tvgm\tindustry\date\n")
        for symbol, rank in stock_ranks.items():
            if isinstance(rank, StockRank):
                f.write(
                    f"{symbol}\t{rank.zacks_rank}\t{rank.value}\t{rank.growth}\t{rank.momentum}\t{rank.vgm}\t{rank.industry}\n"
                )
            else:
                f.write(f"{symbol}\t{rank!r}\n")


if __name__ == "__main__":
    asyncio.run(main())
