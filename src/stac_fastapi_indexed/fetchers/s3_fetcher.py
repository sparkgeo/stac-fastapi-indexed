from asyncio import get_running_loop

from boto3 import client

from stac_fastapi_indexed.fetchers.fetcher import Fetcher
from stac_index_common.data_stores.s3 import get_str_object_from_url, url_prefix_regex


class S3Fetcher(Fetcher):
    @staticmethod
    def compatibility_regex() -> str:
        return url_prefix_regex

    def __init__(self):
        super().__init__()
        self._s3 = client("s3")

    async def fetch(self, url: str) -> str:
        def caller():
            return get_str_object_from_url(self._s3, url)

        return await get_running_loop().run_in_executor(None, caller)
