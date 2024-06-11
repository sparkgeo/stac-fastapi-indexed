from typing import List, Optional

import attr
from fastapi import Request
from stac_fastapi.types.core import AsyncBaseCoreClient
from stac_fastapi.types.rfc3339 import DateTimeType
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection
from stac_pydantic.shared import BBox


@attr.s
class CoreCrudClient(AsyncBaseCoreClient):
    async def all_collections(self, request: Request, **kwargs) -> Collections:
        pass

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
        pass

    async def get_search(  # noqa: C901
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
