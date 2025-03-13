from re import sub
from typing import Any, List, Optional, Tuple

from stac_index.common.index_reader import IndexReader
from stac_index.reader.s3.s3_source_reader import S3SourceReader
from stac_index.reader.s3.settings import get_settings


class S3IndexReader(IndexReader):
    @staticmethod
    def can_handle_source_uri(index_source_uri: str) -> bool:
        return S3SourceReader.can_handle_uri(index_source_uri)

    def __init__(self, index_source_uri: str):
        super().__init__(index_source_uri)
        self._s3_endpoint = get_settings().endpoint
        self._s3_insecure = (
            self._s3_endpoint is not None and self._s3_endpoint.startswith("http://")
        )

    def get_duckdb_configuration_statements(
        self,
    ) -> List[Tuple[str, Optional[List[Any]]]]:
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
        return [
            (
                "CREATE SECRET ({})".format(
                    ", ".join([f"{key} {value}" for key, value in config_parts.items()])
                ),
                None,
            )
        ]
