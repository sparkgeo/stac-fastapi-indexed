from dataclasses import dataclass
from functools import lru_cache
from logging import Logger, getLogger
from typing import Dict, Final

from duckdb import DuckDBPyConnection

_logger: Final[Logger] = getLogger(__file__)


@dataclass
class QueryableConfig:
    items_column: str
    is_geometry: bool


@lru_cache(maxsize=1)
def get_queryable_config_by_name(
    connection: DuckDBPyConnection,
) -> Dict[str, QueryableConfig]:
    _logger.debug("fetching queryable field config")
    field_config = {}
    for row in connection.execute(
        "SELECT name, items_column, is_geometry FROM queryables_by_collection"
    ).fetchall():
        field_config[row[0]] = QueryableConfig(
            items_column=row[1],
            is_geometry=row[2],
        )
    return field_config
