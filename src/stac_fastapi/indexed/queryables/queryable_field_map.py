from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Dict, Final

from async_lru import alru_cache

from stac_fastapi.indexed.db import fetchall, format_query_object_name, get_last_load_id

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


async def get_queryable_config_by_name() -> Dict[str, QueryableConfig]:
    # ensure a change to the application's last load ID forces a data reload
    return await _get_queryable_config_by_name(get_last_load_id())


@alru_cache(maxsize=1)
async def _get_queryable_config_by_name(_: str) -> Dict[str, QueryableConfig]:
    _logger.debug("fetching queryable field config")
    field_config = {}
    for row in await fetchall(
        f"""
        SELECT name
             , qbc.collection_id
             , qbc.description
             , qbc.json_schema
             , qbc.items_column
             , icols.column_type as items_column_type
             , icols.column_type = 'GEOMETRY' as is_geometry
             , icols.column_type IN ('TIMESTAMP WITH TIME ZONE') as is_temporal
          FROM {format_query_object_name('queryables_by_collection')} qbc
    INNER JOIN (DESCRIBE {format_query_object_name('items')}) icols ON qbc.items_column = icols.column_name
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
