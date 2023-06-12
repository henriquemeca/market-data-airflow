from typing import List


def join_urls(url_list: List[str]) -> str:
    temp_url = ""
    for i, url in enumerate(url_list):
        if i == 0:
            temp_url = url
            continue
        if temp_url.endswith("/") and url.startswith("/"):
            temp_url = temp_url[:-1] + url
        elif not temp_url.endswith("/") and not url.startswith("/"):
            temp_url = temp_url + "/" + url
        else:
            temp_url = temp_url + url

    return temp_url
