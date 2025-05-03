from logging import Logger, getLogger
from re import sub
from time import time
from typing import Any, Dict, Final, List, Optional, Self, Tuple

from obstore import Bytes
from obstore.store import S3Store
from stac_index.io.readers.exceptions import UriNotFoundException
from stac_index.io.readers.source_reader import IndexReader, SourceReader
from stac_index.io.s3_common import can_handle_uri as can_handle_uri_common
from stac_index.io.s3_common import get_s3_key_parts, get_settings
from stac_index.io.s3_common import obstore_for_bucket as obstore_for_bucket_common
from stac_index.io.s3_common import path_separator as path_separator_common

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
        return can_handle_uri_common(uri=uri)

    def __init__(self: Self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating S3 Reader")
        self._obstore_cache: Dict[
            str, S3Store
        ] = {}  # stores are bucket-specific, so need one per unique bucket

    def path_separator(self: Self) -> str:
        return path_separator_common()

    def _obstore_for_bucket(self: Self, bucket: str) -> S3Store:
        if bucket not in self._obstore_cache:
            _logger.info(f"creating S3 Reader obstore for bucket '{bucket}'")
            self._obstore_cache[bucket] = obstore_for_bucket_common(bucket=bucket)
        return self._obstore_cache[bucket]

    async def _get_uri_as_bytes(self: Self, uri: str) -> Bytes:
        bucket, key = get_s3_key_parts(uri)
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
        bucket, prefix = get_s3_key_parts(uri)
        uris: List[str] = []
        list_stream = self._obstore_for_bucket(bucket=bucket).list(prefix=prefix)
        async for chunk in list_stream:
            if item_limit and len(uris) >= item_limit:
                break
            for entry in chunk:
                uris.append("s3://{}/{}".format(bucket, entry["path"]))
        return (uris if item_limit is None else uris[:item_limit], [])

    async def get_last_modified_epoch_for_uri(self: Self, uri: str) -> Optional[int]:
        bucket, prefix = get_s3_key_parts(uri)
        try:
            object_meta = await self._obstore_for_bucket(bucket=bucket).head_async(
                path=prefix
            )
        except FileNotFoundError:
            return None
        last_modified = object_meta["last_modified"]
        return round(last_modified.timestamp())

    def get_index_reader(self: Self, index_manifest_uri: str):
        return _S3IndexReader(source_reader=self, index_manifest_uri=index_manifest_uri)
