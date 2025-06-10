from json import loads
from typing import Any, Dict, Optional

from fastapi import Request
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient

from stac_fastapi.indexed.constants import collection_wildcard
from stac_fastapi.indexed.queryables.queryable_field_map import (
    get_queryable_config_by_name,
)


class FiltersClient(AsyncBaseFiltersClient):
    async def get_queryables(
        self,
        request: Request,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        queryables = {}
        for field_config in (await get_queryable_config_by_name()).values():
            if (
                collection_id is None
                and field_config.collection_id == collection_wildcard
            ) or (
                collection_id is not None
                and field_config.collection_id in [collection_id, collection_wildcard]
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
