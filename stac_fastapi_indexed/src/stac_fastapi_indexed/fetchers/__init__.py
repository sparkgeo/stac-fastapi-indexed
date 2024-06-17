from re import match
from typing import Dict, Final

from .fetcher import Fetcher
from .file_fetcher import FileFetcher
from .s3_fetcher import S3Fetcher

_fetchers: Final[Dict[str, Fetcher]] = {
    FileFetcher.compatibility_regex(): FileFetcher(),
    S3Fetcher.compatibility_regex(): S3Fetcher(),
}


async def fetch(url: str) -> str:
    for regex, fetcher in _fetchers.items():
        if match(regex, url):
            return await fetcher.fetch(url)
    raise Exception(f"url format not supported: '{url}'")
