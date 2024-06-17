from abc import ABC, abstractmethod
from typing import Dict, Optional

from duckdb import DuckDBPyConnection


class IndexSource(ABC):
    @classmethod
    @abstractmethod
    def create_index_source(cls, url: str) -> Optional["IndexSource"]:
        pass

    @abstractmethod
    def get_parquet_urls(self) -> Dict[str, str]:
        pass

    def configure_duckdb(self, connection: DuckDBPyConnection) -> None:
        pass
