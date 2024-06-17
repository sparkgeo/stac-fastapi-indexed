from asyncio import get_running_loop

from boto3 import client

from stac_fastapi_indexed.fetchers.fetcher import Fetcher
from stac_fastapi_indexed.settings import get_settings
from stac_index_common.data_stores.s3 import get_str_object_from_url, url_prefix_regex


class S3Fetcher(Fetcher):
    @staticmethod
    def compatibility_regex() -> str:
        return url_prefix_regex

    def __init__(self):
        super().__init__()
        client_args = {}
        s3_endpoint = get_settings().s3_endpoint
        if s3_endpoint is not None:
            client_args["endpoint_url"] = s3_endpoint
            if s3_endpoint.startswith("http://"):
                client_args["use_ssl"] = False
        self._s3 = client("s3", **client_args)

    async def fetch(self, url: str) -> str:
        def caller():
            return get_str_object_from_url(self._s3, url)

        return await get_running_loop().run_in_executor(None, caller)
