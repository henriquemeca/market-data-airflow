def join_url(url: str, uri: str):
    if url.endswith("/") and uri.startswith("/"):
        return url[:-1] + uri
    elif not url.endswith("/") and not uri.startswith("/"):
        return url + "/" + uri

    return url + uri
