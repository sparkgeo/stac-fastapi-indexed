from typing import Any, List, Optional, Tuple

from stac_index.common.index_reader import IndexReader

from .filesystem_source_reader import FilesystemSourceReader


class FilesystemIndexReader(IndexReader):
    @staticmethod
    def can_handle_source_uri(index_source_uri: str) -> bool:
        return FilesystemSourceReader.can_handle_uri(index_source_uri)

    def __init__(self, index_source_uri: str):
        super().__init__(index_source_uri)

    def get_duckdb_configuration_statements(
        self,
    ) -> List[Tuple[str, Optional[List[Any]]]]:
        return []
