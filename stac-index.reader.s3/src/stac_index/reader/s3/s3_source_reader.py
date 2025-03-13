from functools import partial
from logging import Logger, getLogger
from re import Match, match
from time import time
from typing import Final, List, Optional, Tuple, cast

from boto3 import client
from botocore.config import Config as BotoConfig
from stac_index.common.async_util import get_callable_event_loop
from stac_index.common.source_reader import SourceReader
from stac_index.reader.s3.settings import get_settings

_uri_prefix_regex: Final[str] = r"^s3://"
_logger: Final[Logger] = getLogger(__file__)


class S3SourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return not not match(_uri_prefix_regex, uri)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating S3 Reader")
        client_args = {}
        s3_endpoint = get_settings().endpoint
        if s3_endpoint is not None:
            client_args["endpoint_url"] = s3_endpoint
            if s3_endpoint.startswith("http://"):
                client_args["use_ssl"] = False
        if self.reader_concurrency is not None:
            pool_size = self.reader_concurrency * 10
            _logger.info(f"adjusting S3 pool size to {pool_size}")
            client_args["config"] = BotoConfig(max_pool_connections=pool_size)
        self._s3 = client("s3", **client_args)

    def _get_s3_key_parts(self, key: str) -> Tuple[str, str]:
        return cast(Match, match(rf"{_uri_prefix_regex}([^/]+)/(.+)", key)).groups()

    async def get_uri_as_string(self, uri: str) -> str:
        try:
            bucket, key = self._get_s3_key_parts(uri)
        except Exception as e:
            raise ValueError(f"'{uri}' is not in the expected format", e)
        get_object_partial = partial(
            self._s3.get_object,
            Bucket=bucket,
            Key=key,
        )
        try:
            start = time()
            response = (
                (
                    await get_callable_event_loop().run_in_executor(
                        None, get_object_partial
                    )
                )["Body"]
                .read()
                .decode("UTF-8")
            )
        except Exception:
            _logger.exception(f"S3: failed to fetch {uri}")
            raise
        else:
            _logger.debug(
                "S3: fetched '{}/{}' in {}s".format(
                    bucket,
                    key,
                    round(time() - start, 3),
                )
            )
            return response

    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        bucket, prefix = self._get_s3_key_parts(uri)
        next_token = None
        all_keys: List[str] = []
        while True:
            list_kwargs = {
                "Bucket": bucket,
                "Prefix": prefix,
            }
            if next_token:
                list_kwargs["ContinuationToken"] = next_token
            list_objects_partial = partial(
                self._s3.list_objects_v2,
                **list_kwargs,
            )
            response = await get_callable_event_loop().run_in_executor(
                None, list_objects_partial
            )
            if "Contents" in response:
                for object in response["Contents"]:
                    key: str = object["Key"]
                    all_keys.append(f"s3://{bucket}/{key}")
                    if item_limit is not None and len(all_keys) == item_limit:
                        return (all_keys, [])
            if response.get("IsTruncated"):
                next_token = response.get("NextContinuationToken")
            else:
                break
        return (all_keys, [])
