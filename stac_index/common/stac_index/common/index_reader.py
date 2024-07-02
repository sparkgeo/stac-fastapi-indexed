from logging import Logger, getLogger
from re import escape, match
from typing import Dict, Final

from duckdb import DuckDBPyConnection

_logger: Final[Logger] = getLogger(__file__)


class IndexReader:
    @staticmethod
    def can_handle_source_uri(_: str) -> bool:
        return True

    def __init__(self, index_source_uri):
        self._index_source_uri = index_source_uri
        from stac_index.common import source_reader_classes

        for reader_class in source_reader_classes:
            if reader_class.can_handle_uri(index_source_uri):
                self._source_reader = reader_class()
                break
        if self._source_reader is None:
            raise Exception(
                f"unable to locate reader capable of reading '{index_source_uri}'"
            )

    async def get_parquet_uris(self) -> Dict[str, str]:
        uri_dict: Dict[str, str] = {}
        uri_suffix = ".parquet"
        uris = await self._source_reader.list_uris_by_prefix(
            self._index_source_uri, uri_suffix=uri_suffix
        )
        for uri in uris:
            match_result = match(rf".*/([^/]+)({escape(uri_suffix)})$", uri)
            if match_result:
                uri_dict[match_result.group(1)] = uri
        return uri_dict

    def configure_duckdb(self, _: DuckDBPyConnection) -> None:
        _logger.info("not performing any special DuckDB configuration")
        pass
