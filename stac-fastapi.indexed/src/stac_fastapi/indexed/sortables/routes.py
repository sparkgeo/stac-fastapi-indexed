from typing import Final

from fastapi import FastAPI
from stac_fastapi.indexed.constants import collection_wildcard
from stac_fastapi.indexed.sortables.models import SortableField, SortablesResponse
from stac_fastapi.indexed.sortables.sortable_config import get_sortable_configs

_sortables_tag: Final[str] = "Sort Extension"


def add_routes(app: FastAPI) -> None:
    @app.get(
        "/sortables",
        response_model=SortablesResponse,
        tags=[_sortables_tag],
        summary="Sortables",
    )
    async def get_all_sortables() -> SortablesResponse:
        return SortablesResponse(
            fields=[
                SortableField(
                    title=config.name,
                    description=config.description,
                )
                for config in get_sortable_configs()
                if config.collection_id == collection_wildcard
            ]
        )

    @app.get(
        "/collections/{collection_id}/sortables",
        response_model=SortablesResponse,
        tags=[_sortables_tag],
        summary="Collection Sortables",
    )
    async def get_collection_sortables(collection_id: str) -> SortablesResponse:
        return SortablesResponse(
            fields=[
                SortableField(
                    title=config.name,
                    description=config.description,
                )
                for config in get_sortable_configs()
                if config.collection_id in [collection_id, collection_wildcard]
            ]
        )
