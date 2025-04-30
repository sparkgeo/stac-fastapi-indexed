from abc import ABC, abstractmethod
from json import loads
from logging import Logger, getLogger
from typing import Any, Dict, Final, List, Optional, Self, Tuple

from stac_index.indexer.types.index_manifest import IndexManifest
from stac_index.readers.exceptions import MissingIndexException, UriNotFoundException

_logger: Final[Logger] = getLogger(__name__)


class IndexReader:
    def __init__(self: Self, source_reader: "SourceReader", index_manifest_uri: str):
        self._source_reader = source_reader
        self._index_manifest_uri = index_manifest_uri

    async def get_index_manifest(self: Self) -> IndexManifest:
        try:
            return IndexManifest(
                **await self._source_reader.load_json_from_uri(self._index_manifest_uri)
            )
        except UriNotFoundException:
            raise MissingIndexException(f"index missing at {self._index_manifest_uri}")

    async def get_parquet_uris(self: Self) -> Dict[str, str]:
        manifest = await self.get_index_manifest()
        return {
            table_name: "/".join(
                self._index_manifest_uri.split("/")[:-1] + [metadata.relative_path]
            )
            for table_name, metadata in manifest.tables.items()
        }

    def get_duckdb_configuration_statements(
        self: Self,
    ) -> List[Tuple[str, Optional[List[Any]]]]:
        _logger.info("not performing any special DuckDB configuration")
        return []


class SourceReader(ABC):
    def __init__(self: Self, *args, **kwargs):
        concurrency = None
        concurrency_key = "concurrency"
        if concurrency_key in kwargs and isinstance(kwargs[concurrency_key], int):
            concurrency = kwargs["concurrency"]
            if concurrency <= 0:
                concurrency = None
        self.reader_concurrency = concurrency

    @staticmethod
    @abstractmethod
    def can_handle_uri(uri: str) -> bool:
        pass

    @abstractmethod
    def path_separator(self: Self) -> str:
        pass

    @abstractmethod
    async def get_uri_as_string(self: Self, uri: str) -> str:
        pass

    @abstractmethod
    async def get_uri_to_file(self: Self, uri: str, file_path: str) -> None:
        pass

    @abstractmethod
    async def get_item_uris_from_items_uri(
        self: Self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        pass

    @abstractmethod
    async def get_last_modified_epoch_for_uri(self: Self, uri: str) -> int:
        pass

    async def load_json_from_uri(self: Self, uri: str) -> Dict[str, Any]:
        return loads(await self.get_uri_as_string(uri))

    def get_index_reader(self: Self, index_manifest_uri: str) -> IndexReader:
        return IndexReader(source_reader=self, index_manifest_uri=index_manifest_uri)
