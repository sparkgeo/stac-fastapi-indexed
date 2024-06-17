from re import escape, match
from typing import Dict, Optional

from boto3 import client
from duckdb import DuckDBPyConnection

from stac_fastapi_indexed.index_source.index_source import IndexSource
from stac_index_common.data_stores.s3 import (
    get_s3_key_parts,
    list_objects_from_url,
    url_prefix_regex,
)


class S3IndexSource(IndexSource):
    @classmethod
    def create_index_source(cls, url: str) -> Optional[IndexSource]:
        if match(url_prefix_regex, url):
            return cls(url)
        return None

    def __init__(self, base_url):
        super().__init__()
        self._base_url = base_url
        bucket, _ = get_s3_key_parts(self._base_url)
        self._bucket = bucket
        self._s3 = client("s3")

    def get_parquet_urls(self) -> Dict[str, str]:
        urls = {}
        file_suffix = ".parquet"
        keys = list_objects_from_url(self._s3, self._base_url, file_suffix)
        for key in keys:
            match_result = match(rf".*/([^/]+)({escape(file_suffix)})$", key)
            if match_result:
                urls[match_result.group(1)] = f"s3://{self._bucket}/{key}"
        return urls

    def configure_duckdb(self, connection: DuckDBPyConnection) -> None:
        connection.execute("""
            CREATE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN               
            )
        """)
