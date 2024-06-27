from json import loads
from typing import Any, Dict, Final, Optional, cast

from duckdb import DuckDBPyConnection
from fastapi import Request
from stac_fastapi.types.core import AsyncBaseFiltersClient

from stac_fastapi.indexed.search.filter.queryable_field_map import (
    get_queryable_config_by_name,
)

_collection_wildcard: Final[str] = "*"


class FiltersClient(AsyncBaseFiltersClient):
    async def get_queryables(
        self,
        request: Request,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        queryables = {}
        for field_config in get_queryable_config_by_name(
            cast(DuckDBPyConnection, request.app.state.db_connection)
        ).values():
            if (
                collection_id is None
                and field_config.collection_id == _collection_wildcard
            ) or (
                collection_id is not None
                and field_config.collection_id in [collection_id, _collection_wildcard]
            ):
                queryables[field_config.name] = {
                    **loads(field_config.json_schema),
                    "title": field_config.name,
                    "description": field_config.description,
                }
        return {
            "$id": str(request.url),
            "type": "object",
            "title": "STAC Queryables",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": queryables,
            "additionalProperties": True,
        }
