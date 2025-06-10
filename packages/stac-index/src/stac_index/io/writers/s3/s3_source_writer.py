from logging import Logger, getLogger
from typing import Dict, Final, Self

from obstore.store import S3Store
from stac_index.io.s3_common import can_handle_uri as can_handle_uri_common
from stac_index.io.s3_common import get_s3_key_parts
from stac_index.io.s3_common import obstore_for_bucket as obstore_for_bucket_common
from stac_index.io.s3_common import path_separator as path_separator_common
from stac_index.io.writers.source_writer import SourceWriter

_logger: Final[Logger] = getLogger(__name__)


class S3SourceWriter(SourceWriter):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return can_handle_uri_common(uri=uri)

    def __init__(self: Self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating S3 Writer")
        self._obstore_cache: Dict[
            str, S3Store
        ] = {}  # stores are bucket-specific, so need one per unique bucket

    def path_separator(self: Self) -> str:
        return path_separator_common()

    async def put_file_to_uri(self: Self, file_path: str, uri: str) -> None:
        _logger.info(f"uploading {file_path} to {uri}")
        bucket, key = get_s3_key_parts(key=uri)
        obstore = self._obstore_for_bucket(bucket=bucket)
        await obstore.put_async(path=key, file=file_path)

    def _obstore_for_bucket(self: Self, bucket: str) -> S3Store:
        if bucket not in self._obstore_cache:
            _logger.info(f"creating S3 Reader obstore for bucket '{bucket}'")
            self._obstore_cache[bucket] = obstore_for_bucket_common(bucket=bucket)
        return self._obstore_cache[bucket]
