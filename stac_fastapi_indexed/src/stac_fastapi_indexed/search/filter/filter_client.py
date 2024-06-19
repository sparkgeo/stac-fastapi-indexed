from json import loads
from typing import Any, Dict, Optional, cast

from duckdb import DuckDBPyConnection
from fastapi import Request
from stac_fastapi.types.core import AsyncBaseFiltersClient


class FiltersClient(AsyncBaseFiltersClient):
    async def get_queryables(
        self,
        request: Request,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        queryables = {}
        for row in (
            cast(DuckDBPyConnection, request.app.state.db_connection)
            .execute(
                """
            SELECT name
                 , description
                 , json_schema
              FROM queryables_by_collection
                {}
        """.format(
                    f"WHERE collection_id = '{collection_id}'"
                    if collection_id is not None
                    else ""
                )
            )
            .fetchall()
        ):
            queryables[row[0]] = {
                **loads(row[2]),
                "title": row[0],
                "description": row[1],
            }
        return {
            "$id": str(request.url),
            "type": "object",
            "title": "STAC Queryables",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": queryables,
            "additionalProperties": True,
        }
