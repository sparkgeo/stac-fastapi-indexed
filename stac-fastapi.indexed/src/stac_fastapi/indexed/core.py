from asyncio import gather
from json import loads
from logging import Logger, getLogger
from re import IGNORECASE, match, search
from typing import Final, List, Optional, cast
from urllib.parse import unquote_plus

import attr
from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from stac_fastapi.indexed.constants import rel_parent, rel_root, rel_self
from stac_fastapi.indexed.db import fetchall, fetchone
from stac_fastapi.indexed.links.catalog import get_catalog_link
from stac_fastapi.indexed.links.collection import (
    fix_collection_links,
    get_collections_link,
)
from stac_fastapi.indexed.links.item import fix_item_links
from stac_fastapi.indexed.search.filter.parser import FilterLanguage
from stac_fastapi.indexed.search.search_handler import SearchHandler
from stac_fastapi.indexed.stac.fetcher import fetch_dict
from stac_fastapi.types.core import AsyncBaseCoreClient
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.rfc3339 import DateTimeType
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection
from stac_pydantic.shared import BBox

from stac_index.common.stac_parser import StacParser

_logger: Final[Logger] = getLogger(__file__)


@attr.s
class CoreCrudClient(AsyncBaseCoreClient):
    async def all_collections(self, request: Request, **kwargs) -> Collections:
        # Alter how call is answered based on who is asking.
        # Catalog root requests (/) requires a link for each collection, but doesn't use any other collection data.
        # All Collections requests (/collections) requires all data about all collections.
        # Because collection data comes from JSON stored externally that must be retrieved, we should only get all collection data when actually required.
        # If this request is to satisfy a Catalog root request, get the minimum information required to satisfy that request (collection IDs).
        if (
            request.url.path.replace(cast(FastAPI, request.scope["app"]).root_path, "")
            == "/"
        ):
            _logger.debug(f"answering '{request.url}' as minimal collections response")
            return self._get_minimal_collections_response()
        else:
            _logger.debug(f"answering '{request.url}' as full collections response")
            return await self._get_full_collections_response(request)

    async def get_collection(
        self, collection_id: str, request: Request, **kwargs
    ) -> Collection:
        row = fetchone(
            "SELECT stac_location FROM collections WHERE id = ?",
            [collection_id],
        )
        if row is not None:
            return fix_collection_links(
                Collection(**await fetch_dict(row[0])),
                request,
            )
        raise NotFoundError(f"Collection {collection_id} does not exist.")

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
        try:
            search_request = self.post_request_model(
                **{
                    key: value
                    for key, value in {
                        "collections": [collection_id],
                        "bbox": bbox,
                        "datetime": datetime,
                        "limit": limit,
                        "token": token,
                    }.items()
                    if value is not None
                }
            )
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided {e}"
            ) from e
        return await SearchHandler(
            search_request=search_request, request=request
        ).search()

    async def get_item(
        self, item_id: str, collection_id: str, request: Request, **kwargs
    ) -> Item:
        await self.get_collection(
            collection_id, request=request
        )  # will error if collection does not exist
        row = fetchone(
            "SELECT stac_location, applied_fixes FROM items WHERE collection_id = ? and id = ?",
            [collection_id, item_id],
        )
        if row is not None:
            return fix_item_links(
                Item(
                    StacParser(row[1].split(",")).parse_stac_item(
                        await fetch_dict(row[0])
                    )[1]
                ),
                request,
            )
        raise NotFoundError(
            f"Item {item_id} in Collection {collection_id} does not exist."
        )

    async def post_search(
        self, search_request: BaseSearchPostRequest, request: Request, **kwargs
    ) -> ItemCollection:
        return await SearchHandler(
            search_request=search_request, request=request
        ).search()

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
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
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "datetime": datetime,
            "limit": limit,
            "token": token,
        }
        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sortparts = match(r"^([+-]?)(.*)$", sort)
                if sortparts:
                    sort_param.append(
                        {
                            "field": sortparts.group(2).strip(),
                            "direction": "desc" if sortparts.group(1) == "-" else "asc",
                        }
                    )
            base_args["sortby"] = sort_param
        if intersects:
            base_args["intersects"] = loads(unquote_plus(intersects))
        if filter:
            # following block based on https://github.com/stac-utils/stac-fastapi-pgstac/blob/659ddc374b7001dc7c7ad2cc2fd29e3f420b0573/stac_fastapi/pgstac/core.py#L373
            # Kludgy fix because using factory does not allow alias for filter-lang
            if filter_lang is None:
                lang_match = search(
                    r"filter-lang=([a-z0-9-]+)", str(request.query_params), IGNORECASE
                )
                if lang_match:
                    filter_lang = lang_match.group(1)
            filter_lang = filter_lang or FilterLanguage.TEXT.value
            # prefer to wrap / unwrap filter content here than parse, convert, and re-parse
            base_args["filter"] = SearchHandler.wrap_text_filter(filter, filter_lang)
            base_args["filter-lang"] = filter_lang
        try:
            search_request = self.post_request_model(
                **{
                    key: value
                    for key, value in base_args.items()
                    if value is not None and value != []
                }
            )
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided {e}"
            ) from e

        return await SearchHandler(
            search_request=search_request, request=request
        ).search()

    def _get_minimal_collections_response(self) -> Collections:
        return Collections(
            collections=[
                Collection(**{"id": id})
                for id in [
                    row[0] for row in fetchall("SELECT id FROM collections ORDER BY id")
                ]
            ],
            links=[],
        )

    async def _get_full_collections_response(self, request: Request) -> Collections:
        fetch_tasks = [
            fetch_dict(url)
            for url in [
                row[0]
                for row in fetchall("SELECT stac_location FROM collections ORDER BY id")
            ]
        ]
        collections = [
            fix_collection_links(
                Collection(**collection_dict),
                request,
            )
            for collection_dict in await gather(*fetch_tasks)
        ]
        return Collections(
            collections=collections,
            links=[
                get_catalog_link(request, rel_root),
                get_catalog_link(request, rel_parent),
                get_collections_link(request, rel_self),
            ],
        )
