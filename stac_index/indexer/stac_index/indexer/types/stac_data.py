from pydantic import BaseModel
from stac_pydantic import Collection, Item


class WithLocation(BaseModel):
    location: str


class CollectionWithLocation(Collection, WithLocation):
    pass


class ItemWithLocation(Item, WithLocation):
    pass
