from enum import Enum
from typing import Any, Dict, Final, Optional

from pydantic import BaseModel

collection_wildcard: Final[str] = "*"


class StorageTypeProperties(BaseModel):
    pass


class StorageType(str, Enum):
    DOUBLE = "DOUBLE"


class Queryable(BaseModel):
    storage_type: StorageType
    storage_type_properties: Optional[StorageTypeProperties] = None
    description: str
    json_path: str
    json_schema: Dict[str, Any]
    is_geometry: Optional[bool] = False
    is_temporal: Optional[bool] = False


QueryablesByFieldName = Dict[str, Queryable]
QueryablesByCollection = Dict[str, QueryablesByFieldName]


class Queryables(BaseModel):
    collection: QueryablesByCollection


class Sortable(BaseModel):
    description: str


SortablesByFieldName = Dict[str, Sortable]
SortablesByCollection = Dict[str, SortablesByFieldName]


class Sortables(BaseModel):
    collection: SortablesByCollection


class IndexConfig(BaseModel):
    root_catalog_uri: str
    queryables: Queryables
    sortables: Sortables
