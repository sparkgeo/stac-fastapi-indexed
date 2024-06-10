from dataclasses import dataclass, field
from typing import List

from pydantic import BaseModel
from stac_pydantic import Catalog, Collection, Item

from stac_indexer.types.data_access_type import DataAccessType


class WithLocation(BaseModel):
    location: str


class CatalogWithLocation(Catalog, WithLocation):
    pass


class CollectionWithLocation(Collection, WithLocation):
    pass


class ItemWithLocation(Item, WithLocation):
    pass


@dataclass
class StacData:
    data_access_type: DataAccessType
    root_catalog: CatalogWithLocation
    catalogs: List[CatalogWithLocation] = field(default_factory=list)
    collections: List[CollectionWithLocation] = field(default_factory=list)
    items: List[ItemWithLocation] = field(default_factory=list)
