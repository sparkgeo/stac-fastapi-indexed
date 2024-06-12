from asyncio import gather
from json import loads
from logging import Logger, getLogger
from typing import Final, List, Optional, cast

import attr
from duckdb import DuckDBPyConnection
from fastapi import Request
from stac_fastapi.types.core import AsyncBaseCoreClient
from stac_fastapi.types.rfc3339 import DateTimeType
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection
from stac_pydantic.shared import BBox

from stac_fastapi_indexed.constants import rel_parent, rel_root, rel_self
from stac_fastapi_indexed.fetchers import fetch
from stac_fastapi_indexed.links.catalog import get_catalog_link
from stac_fastapi_indexed.links.collection import (
    fix_collection_links,
    get_collections_link,
)
from stac_fastapi_indexed.search.search_handler import SearchHandler

_logger: Final[Logger] = getLogger(__file__)


@attr.s
class CoreCrudClient(AsyncBaseCoreClient):
    async def all_collections(self, request: Request, **kwargs) -> Collections:
        fetch_tasks = [
            fetch(url)
            for url in [
                row[0]
                for row in cast(DuckDBPyConnection, request.app.state.db_connection)
                .execute("SELECT stac_location FROM collections")
                .fetchall()
            ]
        ]
        collections = [
            fix_collection_links(
                Collection(**loads(collection_json)),
                request,
            )
            for collection_json in await gather(*fetch_tasks)
        ]
        return Collections(
            collections=collections,
            links=[
                get_catalog_link(request, rel_root),
                get_catalog_link(request, rel_parent),
                get_collections_link(request, rel_self),
            ],
        )

    async def get_collection(
        self, collection_id: str, request: Request, **kwargs
    ) -> Collection:
        pass

    async def item_collection(
        self,
        collection_id: str,
        request: Request,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = None,
        token: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        pass

    async def get_item(
        self, item_id: str, collection_id: str, request: Request, **kwargs
    ) -> Item:
        pass

    async def post_search(
        self, search_request: BaseSearchPostRequest, request: Request, **kwargs
    ) -> ItemCollection:
        return await self._base_search(search_request, request)

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = None,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        intersects: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        pass

    async def _base_search(
        self, search_request: BaseSearchPostRequest, request: Request
    ) -> ItemCollection:
        return await SearchHandler(
            search_request=search_request, request=request
        ).search()
