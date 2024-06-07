from dataclasses import dataclass, field
from typing import List

from stac_pydantic import Catalog, Collection, Item


@dataclass
class StacData:
    root_catalog: Catalog
    catalogs: List[Catalog] = field(default_factory=list)
    collections: List[Collection] = field(default_factory=list)
    items: List[Item] = field(default_factory=list)
