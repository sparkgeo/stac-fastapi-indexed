from dataclasses import dataclass
from functools import lru_cache
from logging import Logger, getLogger
from typing import Dict, Final, List

from stac_fastapi.indexed.db import fetchall, format_query_object_name

_logger: Final[Logger] = getLogger(__name__)


@dataclass
class SortableConfig:
    name: str
    collection_id: str
    description: str
    items_column: str


@lru_cache(maxsize=1)
def get_sortable_configs() -> List[SortableConfig]:
    _logger.debug("fetching sortable field config")
    return [
        SortableConfig(
            name=row[0],
            collection_id=row[1],
            description=row[2],
            items_column=row[3],
        )
        for row in fetchall(
            f"""
        SELECT name
             , collection_id
             , description
             , items_column
          FROM {format_query_object_name('sortables_by_collection')}
    """
        )
    ]


def get_sortable_configs_by_field() -> Dict[str, SortableConfig]:
    return {config.name: config for config in get_sortable_configs()}
