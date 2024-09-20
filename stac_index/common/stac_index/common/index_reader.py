from logging import Logger, getLogger
from typing import Any, Dict, Final, List, Optional, Tuple

from stac_index.common.index_manifest import IndexManifest

_logger: Final[Logger] = getLogger(__file__)


class IndexReader:
    @staticmethod
    def can_handle_source_uri(_: str) -> bool:
        return True

    def __init__(self, index_manifest_uri: str):
        self.index_manifest_uri = index_manifest_uri
        from stac_index.common import source_reader_classes

        for reader_class in source_reader_classes:
            if reader_class.can_handle_uri(index_manifest_uri):
                self._source_reader = reader_class()
                break
        if self._source_reader is None:
            raise Exception(
                f"unable to locate reader capable of reading '{index_manifest_uri}'"
            )

    async def get_parquet_uris(self) -> Dict[str, str]:
        manifest = IndexManifest(
            **await self._source_reader.load_json_from_uri(self.index_manifest_uri)
        )
        print(manifest)
        return {
            table_name: "/".join(
                self.index_manifest_uri.split("/")[:-1] + [metadata.relative_path]
            )
            for table_name, metadata in manifest.tables.items()
        }

    def get_duckdb_configuration_statements(
        self,
    ) -> List[Tuple[str, Optional[List[Any]]]]:
        _logger.info("not performing any special DuckDB configuration")
        return []
