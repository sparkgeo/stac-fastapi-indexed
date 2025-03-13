from typing import Optional, Set

from pydantic import BaseModel
from stac_pydantic import Collection, Item


class WithLocation(BaseModel):
    location: str


class WithFixes(BaseModel):
    applied_fixes: Optional[Set[str]] = None


class CollectionWithLocation(Collection, WithLocation):
    pass


class ItemWithLocation(Item, WithLocation, WithFixes):
    pass
