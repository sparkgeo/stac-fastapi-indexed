from enum import Enum
from re import sub
from typing import Any, Dict, Final, List, Optional

from pydantic import BaseModel

collection_wildcard: Final[str] = "*"


class StorageTypeProperties(BaseModel):
    pass


class StorageType(str, Enum):
    DOUBLE = "DOUBLE"


class Indexable(BaseModel):
    json_path: str
    description: str
    storage_type: StorageType
    storage_type_properties: Optional[StorageTypeProperties] = None

    @property
    def table_column_name(self) -> str:
        return "i_{}".format(sub("[^A-Za-z0-9]", "_", self.json_path))


class Queryable(BaseModel):
    json_schema: Dict[str, Any]
    collections: List[str]


class Sortable(BaseModel):
    collections: List[str]


IndexableByFieldName = Dict[str, Indexable]
QueryableByFieldName = Dict[str, Queryable]
SortablesByFieldName = Dict[str, Sortable]
IndexableByCollection = Dict[str, IndexableByFieldName]


class IndexConfig(BaseModel):
    indexables: IndexableByFieldName = {}
    queryables: QueryableByFieldName = {}
    sortables: SortablesByFieldName = {}
    fixes_to_apply: List[str] = []

    def __init__(self, **data):
        super().__init__(**data)
        for name in self.queryables.keys():
            assert (
                name in self.indexables
            ), "queryable contains name that is not indexed"
        for name in self.sortables.keys():
            assert name in self.indexables, "sortable contains name that is not indexed"

    @property
    def all_indexables_by_collection(self) -> IndexableByCollection:
        by_collection: IndexableByCollection = {}
        for name, queryable in self.queryables.items():
            for collection in queryable.collections:
                if collection not in by_collection:
                    by_collection[collection] = {}
                if name not in by_collection[collection]:
                    by_collection[collection][name] = self.indexables[name]
        for name, sortable in self.sortables.items():
            for collection in sortable.collections:
                if collection not in by_collection:
                    by_collection[collection] = {}
                if name not in by_collection[collection]:
                    by_collection[collection][name] = self.indexables[name]
        return by_collection
