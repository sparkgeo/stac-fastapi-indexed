from dataclasses import dataclass
from functools import lru_cache
from logging import Logger, getLogger
from typing import Final, List

from stac_fastapi.indexed.db import fetchall

_logger: Final[Logger] = getLogger(__file__)


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
            """
        SELECT name
             , collection_id
             , description
             , items_column
          FROM sortables_by_collection
    """
        )
    ]
