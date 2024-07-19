from dataclasses import dataclass
from functools import lru_cache
from logging import Logger, getLogger
from typing import Dict, Final

from stac_fastapi.indexed.db import fetchall

_logger: Final[Logger] = getLogger(__file__)


@dataclass
class QueryableConfig:
    name: str
    collection_id: str
    description: str
    json_schema: str
    items_column: str
    is_geometry: bool
    is_temporal: bool


@lru_cache(maxsize=1)
def get_queryable_config_by_name() -> Dict[str, QueryableConfig]:
    _logger.debug("fetching queryable field config")
    field_config = {}
    for row in fetchall(
        """
        SELECT name
             , collection_id
             , description
             , json_schema
             , items_column
             , is_geometry
             , is_temporal
          FROM queryables_by_collection
    """,
    ):
        field_config[row[0]] = QueryableConfig(
            name=row[0],
            collection_id=row[1],
            description=row[2],
            json_schema=row[3],
            items_column=row[4],
            is_geometry=row[5],
            is_temporal=row[6],
        )
    return field_config
