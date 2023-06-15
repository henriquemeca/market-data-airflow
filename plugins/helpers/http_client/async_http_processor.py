import asyncio
import logging
import random
from typing import Any, Callable, Coroutine, Dict, List, Tuple

import aiohttp

response_processor_type = Callable[
    [str, aiohttp.ClientResponse], Coroutine[Any, Any, Tuple[str, Any]]
]


class AsyncHTTPProcessor:
    def __init__(
        self,
        id_url_dict: Dict[str, str],
        headers: Dict[str, str],
        response_processor: response_processor_type,
        retries=5,
        timeout_seconds: int = 10 * 60,
        sleep_seconds=random.random() * 10,
    ) -> None:
        self.headers = headers
        self.id_url_dict = id_url_dict
        self.response_processor = response_processor
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds

    def process(self) -> Dict[str, Any]:
        return asyncio.run(self.__assync_request_iterator())

    async def __assync_request_iterator(
        self,
    ) -> Dict[str, Any]:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(self.timeout_seconds)
        ) as session:
            awaitables = []
            for id, url in self.id_url_dict.items():
                processed_response = await self.__retry(
                    fun=self.__process_response,
                    try_number=0,
                    session=session,
                    id=id,
                    url=url,
                )
                awaitables.append(processed_response)
            results: List[Tuple[str, Any]] = await asyncio.gather(*awaitables)
        return dict(results)

    async def __process_response(
        self,
        session: aiohttp.ClientSession,
        id: str,
        url: str,
    ) -> Tuple[str, Any]:
        # Sleeping to not overload server
        await asyncio.sleep(self.sleep_seconds)
        async with session.get(url=url, headers=self.headers) as response:
            return await self.response_processor(id, response)

    async def __retry(
        self, fun: Callable, try_number: int, **kwargs
    ) -> aiohttp.ClientResponse:
        """Retry a function if it fails."""
        try:
            return fun(**kwargs)
        except:
            logging.error(
                f"An erro was raised:, during the execution of function:{fun} with  arguments{kwargs}"
            )
            if try_number < self.retries:
                await asyncio.sleep(self.sleep_seconds)
                return await self.__retry(fun=fun, try_number=try_number + 1)
            else:
                logging.error(
                    f"After {self.retries} attempts another error apeared while running function:{fun} with arguments:{kwargs}"
                )
                return Exception("any")
