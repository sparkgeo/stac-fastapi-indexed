from dataclasses import dataclass
from functools import lru_cache
from logging import Logger, getLogger
from typing import Dict, Final

from stac_fastapi.indexed.db import fetchall

_logger: Final[Logger] = getLogger(__name__)


@dataclass
class QueryableConfig:
    name: str
    collection_id: str
    description: str
    json_schema: str
    items_column: str
    items_column_type: str
    is_geometry: bool
    is_temporal: bool


@lru_cache(maxsize=1)
def get_queryable_config_by_name() -> Dict[str, QueryableConfig]:
    _logger.debug("fetching queryable field config")
    field_config = {}
    for row in fetchall(
        """
        SELECT name
             , qbc.collection_id
             , qbc.description
             , qbc.json_schema
             , qbc.items_column
             , icols.column_type as items_column_type
             , icols.column_type = 'GEOMETRY' as is_geometry
             , icols.column_type IN ('TIMESTAMP WITH TIME ZONE') as is_temporal
          FROM queryables_by_collection qbc
    INNER JOIN (DESCRIBE items) icols ON qbc.items_column = icols.column_name
    """,
    ):
        field_config[row[0]] = QueryableConfig(
            name=row[0],
            collection_id=row[1],
            description=row[2],
            json_schema=row[3],
            items_column=row[4],
            items_column_type=row[5],
            is_geometry=row[6],
            is_temporal=row[7],
        )
    return field_config
