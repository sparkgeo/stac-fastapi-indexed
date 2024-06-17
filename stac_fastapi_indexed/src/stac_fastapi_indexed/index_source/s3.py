from logging import Logger, getLogger
from re import escape, match, sub
from typing import Dict, Final, Optional

from boto3 import client
from duckdb import DuckDBPyConnection

from stac_fastapi_indexed.index_source.index_source import IndexSource
from stac_fastapi_indexed.settings import get_settings
from stac_index_common.data_stores.s3 import (
    get_s3_key_parts,
    list_objects_from_url,
    url_prefix_regex,
)

_logger: Final[Logger] = getLogger(__file__)


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
        client_args = {}
        self._s3_endpoint = get_settings().s3_endpoint
        self._s3_insecure = (
            self._s3_endpoint is not None and self._s3_endpoint.startswith("http://")
        )
        if self._s3_endpoint is not None:
            client_args["endpoint_url"] = self._s3_endpoint
            if self._s3_insecure:
                client_args["use_ssl"] = False
        self._s3 = client("s3", **client_args)

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
        config_parts = {
            "TYPE": "S3",
            "PROVIDER": "CREDENTIAL_CHAIN",
        }
        if self._s3_endpoint is not None:
            config_parts["ENDPOINT"] = "'{}'".format(
                sub(r"^.+://", "", self._s3_endpoint)
            )
            config_parts["URL_STYLE"] = "'path'"
        if self._s3_insecure:
            config_parts["USE_SSL"] = "false"
        command = "CREATE SECRET ({})".format(
            ", ".join([f"{key} {value}" for key, value in config_parts.items()])
        )
        _logger.debug(command)
        connection.execute(command)
