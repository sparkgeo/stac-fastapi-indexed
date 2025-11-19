from pydantic import BaseModel
from stac_pydantic.collection import Collection
from stac_pydantic.item import Item


class CollectionWithLocation(BaseModel):
    collection: Collection
    location: str


class ItemWithLocationAndFixes(BaseModel):
    item: Item
    location: str
    applied_fixes: set[str]
