import logging
from typing import Dict, Tuple

import aiohttp
from google.cloud import storage
from helpers.join_url import join_urls


async def get_response_json(
    id: str, response: aiohttp.ClientResponse
) -> Tuple[str, str]:
    return (id, await response.text())


def load_json_responses_to_gcs(
    responses: Dict[str, str], destination_folder: str, bucket: storage.Bucket
) -> None:
    for ticker, json_data in responses.items():
        destination_file = join_urls([destination_folder, f"{ticker}.json"])
        blob = bucket.blob(destination_file)
        blob.upload_from_string(json_data, content_type="application/json")
        logging.info(f"{destination_file} was loaded.")
