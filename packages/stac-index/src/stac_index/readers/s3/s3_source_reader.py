from logging import Logger, getLogger
from re import Match, match, sub
from time import time
from typing import Any, Dict, Final, List, Optional, Self, Tuple, cast

from obstore import Bytes
from obstore.store import S3Store
from stac_index.readers.exceptions import UriNotFoundException
from stac_index.readers.source_reader import IndexReader, SourceReader

from .settings import get_settings

_uri_prefix_regex: Final[str] = r"^s3://"
_logger: Final[Logger] = getLogger(__name__)


class _S3IndexReader(IndexReader):
    def get_duckdb_configuration_statements(
        self,
    ) -> List[Tuple[str, Optional[List[Any]]]]:
        s3_endpoint = get_settings().endpoint
        s3_insecure = s3_endpoint is not None and s3_endpoint.startswith("http://")
        config_parts = {
            "TYPE": "S3",
            "PROVIDER": "CREDENTIAL_CHAIN",
        }
        if s3_endpoint is not None:
            config_parts["ENDPOINT"] = "'{}'".format(sub(r"^.+://", "", s3_endpoint))
            config_parts["URL_STYLE"] = "'path'"
        if s3_insecure:
            config_parts["USE_SSL"] = "false"
        return [
            (
                "CREATE SECRET ({})".format(
                    ", ".join([f"{key} {value}" for key, value in config_parts.items()])
                ),
                None,
            )
        ]


class S3SourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return not not match(_uri_prefix_regex, uri)

    def __init__(self: Self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._obstore_cache: Dict[
            str, S3Store
        ] = {}  # stores are bucket-specific, so need one per unique bucket

    def path_separator(self: Self) -> str:
        return "/"

    def _obstore_for_bucket(self: Self, bucket: str) -> S3Store:
        if bucket not in self._obstore_cache:
            _logger.info(f"creating S3 Reader obstore for bucket '{bucket}'")
            client_config = {}
            store_config = {}
            s3_endpoint = get_settings().endpoint
            if s3_endpoint is not None:
                store_config["endpoint"] = s3_endpoint
                if s3_endpoint.startswith("http://"):
                    client_config["allow_http"] = True
            self._obstore_cache[bucket] = S3Store(
                bucket=bucket, config=store_config, client_options=client_config
            )
        return self._obstore_cache[bucket]

    def _get_s3_key_parts(self: Self, key: str) -> Tuple[str, str]:
        try:
            return cast(Match, match(rf"{_uri_prefix_regex}([^/]+)/(.+)", key)).groups()
        except Exception as e:
            raise ValueError(f"'{key}' is not in the expected format", e)

    async def _get_uri_as_bytes(self: Self, uri: str) -> Bytes:
        bucket, key = self._get_s3_key_parts(uri)
        try:
            start = time()
            object_bytes = await (
                await self._obstore_for_bucket(bucket=bucket).get_async(path=key)
            ).bytes_async()
            _logger.debug(
                "S3: fetched '{}/{}' in {}s".format(
                    bucket,
                    key,
                    round(time() - start, 3),
                )
            )
            return object_bytes
        except FileNotFoundError:
            raise UriNotFoundException(uri)
        except Exception:
            _logger.exception(f"S3: failed to fetch {uri}")
            raise

    async def get_uri_as_string(self: Self, uri: str) -> str:
        return (await self._get_uri_as_bytes(uri)).to_bytes().decode("UTF-8")

    async def get_uri_to_file(self: Self, uri, file_path: str):
        chunk_size = 1000000
        source = await self._get_uri_as_bytes(uri)
        with open(file_path, "wb") as f:
            offset = 0
            while offset < len(source):
                chunk = source[offset : offset + chunk_size]
                f.write(chunk)
                offset += chunk_size

    async def get_item_uris_from_items_uri(
        self: Self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        bucket, prefix = self._get_s3_key_parts(uri)
        uris: List[str] = []
        list_stream = self._obstore_for_bucket(bucket=bucket).list(prefix=prefix)
        async for chunk in list_stream:
            if item_limit and len(uris) >= item_limit:
                break
            for entry in chunk:
                uris.append("s3://{}/{}".format(bucket, entry["path"]))
        return (uris if item_limit is None else uris[:item_limit], [])

    async def get_last_modified_epoch_for_uri(self: Self, uri: str) -> int:
        bucket, prefix = self._get_s3_key_parts(uri)
        object_meta = await self._obstore_for_bucket(bucket=bucket).head_async(
            path=prefix
        )
        last_modified = object_meta["last_modified"]
        return round(last_modified.timestamp())

    def get_index_reader(self: Self, index_manifest_uri: str):
        return _S3IndexReader(source_reader=self, index_manifest_uri=index_manifest_uri)
