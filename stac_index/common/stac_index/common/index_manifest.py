from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, field_serializer


class TablePartitioning(BaseModel):
    partition_fields: List[str]


class TableMetadata(BaseModel):
    relative_path: str
    partitioning: Optional[TablePartitioning] = None


class IndexManifest(BaseModel):
    created: datetime
    tables: Dict[str, TableMetadata] = {}

    @field_serializer("created")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        return timestamp.isoformat()
