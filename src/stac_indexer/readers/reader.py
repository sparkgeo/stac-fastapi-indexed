from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Tuple

from stac_fastapi.types.stac import Catalog

from stac_indexer.types.stac_data import (
    Collection,
    CollectionWithLocation,
    ItemWithLocation,
)


class Reader(ABC):
    @classmethod
    @abstractmethod
    def create_reader(cls, url: str) -> Optional["Reader"]:
        pass

    @abstractmethod
    def get_root_catalog(self) -> Catalog:
        pass

    @abstractmethod
    def get_collections(
        self, root_catalog: Catalog
    ) -> Tuple[List[CollectionWithLocation], List[str]]:
        pass

    @abstractmethod
    def process_items(
        self,
        collections: List[Collection],
        item_ingestor: Callable[[ItemWithLocation], List[str]],
    ) -> List[str]:
        pass
