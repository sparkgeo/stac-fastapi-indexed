from logging import Logger, getLogger
from typing import Final

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

    def configure_duckdb(self, _: DuckDBPyConnection) -> None:
        _logger.info("not performing any special DuckDB configuration")
        pass

    @property
    def source_reader(self):
        return self._source_reader
