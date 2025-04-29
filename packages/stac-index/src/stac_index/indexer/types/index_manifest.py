from datetime import datetime
from typing import Dict, Optional, Self

from pydantic import BaseModel, field_serializer
from stac_index.indexer.types.index_config import IndexConfig


class TableMetadata(BaseModel):
    relative_path: str


class IndexManifest(BaseModel):
    indexer_version: int
    updated: datetime
    load_id: str
    root_catalog_uri: Optional[str] = None
    index_config: Optional[IndexConfig] = None
    tables: Dict[str, TableMetadata] = {}

    @field_serializer("updated")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        return timestamp.isoformat()

    @property
    def is_updateable(self: Self) -> bool:
        return self.root_catalog_uri is not None
