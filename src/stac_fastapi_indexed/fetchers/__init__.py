from re import match
from typing import Dict, Final

from .fetcher import Fetcher
from .file_fetcher import FileFetcher

_fetchers: Final[Dict[str, Fetcher]] = {
    FileFetcher.compatibility_regex(): FileFetcher(),
}


async def fetch(url: str) -> str:
    for regex, fetcher in _fetchers.items():
        if match(regex, url):
            return await fetcher.fetch(url)
    raise Exception(f"url format not supported: '{url}'")
