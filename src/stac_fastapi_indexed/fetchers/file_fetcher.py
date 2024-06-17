from stac_fastapi_indexed.fetchers.fetcher import Fetcher
from stac_index_common.data_stores.file import url_prefix


class FileFetcher(Fetcher):
    @staticmethod
    def compatibility_regex() -> str:
        return url_prefix

    async def fetch(self, url: str) -> str:
        with open(url, "r") as f:
            return f.read()
