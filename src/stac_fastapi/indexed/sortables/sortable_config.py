from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Dict, Final, List

from async_lru import alru_cache

from stac_fastapi.indexed.db import fetchall, format_query_object_name, get_last_load_id

_logger: Final[Logger] = getLogger(__name__)


@dataclass
class SortableConfig:
    name: str
    collection_id: str
    description: str
    items_column: str


async def get_sortable_configs() -> List[SortableConfig]:
    # ensure a change to the application's last load ID forces a data reload
    return await _get_sortable_configs(get_last_load_id())


@alru_cache(maxsize=1)
async def _get_sortable_configs(_: str) -> List[SortableConfig]:
    _logger.debug("fetching sortable field config")
    return [
        SortableConfig(
            name=row[0],
            collection_id=row[1],
            description=row[2],
            items_column=row[3],
        )
        for row in await fetchall(
            f"""
        SELECT name
             , collection_id
             , description
             , items_column
          FROM {format_query_object_name('sortables_by_collection')}
    """
        )
    ]


async def get_sortable_configs_by_field() -> Dict[str, SortableConfig]:
    return {config.name: config for config in await get_sortable_configs()}
