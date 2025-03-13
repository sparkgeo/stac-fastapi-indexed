from datetime import datetime
from typing import Dict

from pydantic import BaseModel, field_serializer


class TableMetadata(BaseModel):
    relative_path: str


class IndexManifest(BaseModel):
    created: datetime
    tables: Dict[str, TableMetadata] = {}

    @field_serializer("created")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        return timestamp.isoformat()
