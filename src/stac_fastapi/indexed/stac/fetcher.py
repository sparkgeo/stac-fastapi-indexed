from asyncio import Semaphore, gather
from json import loads
from logging import Logger, getLogger
from typing import Any, Dict, Final, List, NamedTuple, cast

from fastapi import Request
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.stac import Collection, Item
from stac_index.indexer.stac_parser import StacParser
from stac_index.io.readers import get_reader_for_uri
from stac_index.io.readers.exceptions import UriNotFoundException

from stac_fastapi.indexed.db import fetchall, fetchone, format_query_object_name
from stac_fastapi.indexed.links.collection import fix_collection_links
from stac_fastapi.indexed.links.item import fix_item_links
from stac_fastapi.indexed.settings import get_settings

_logger: Final[Logger] = getLogger(__name__)

# If we ever support multi-catalog indexes, or if we see catalogs
# whose STAC items come from different domains, it might be worth
# maintaining one semaphore per domain as a single semaphore may
# unnecessarily limit concurrency when hosts could handle more.
_semaphore: Final[Semaphore] = Semaphore(value=get_settings().max_concurrency)


class ItemQueryRow(NamedTuple):
    item_content: str | None
    stac_location: str
    applied_fixes: str | None


class CollectionQueryRow(NamedTuple):
    collection_content: str | None
    stac_location: str


async def get_single_item(
    collection_id: str,
    item_id: str,
    request: Request,
    verify_collection_exists: bool = False,
) -> Item:
    if verify_collection_exists:
        await get_single_collection(
            collection_id=collection_id,
            request=request,
        )  # will error if collection does not exist
    row = ItemQueryRow(
        *await fetchone(
            f"SELECT item_content, stac_location, applied_fixes FROM {format_query_object_name('items')} WHERE collection_id = ? and id = ?",
            [collection_id, item_id],
        )
    )
    if row is None:
        raise NotFoundError(
            "Item {item_id} in Collection {collection_id} does not exist.".format(
                item_id=item_id, collection_id=collection_id
            )
        )

    return cast(
        Item, await _get_item_from_query_row(row=row, request=request)
    )  # result cannot be None with error_on_missing as default


async def get_items_from_query_rows(
    rows: list[ItemQueryRow], request: Request
) -> List[Item]:
    return [
        item
        for item in await gather(
            *[
                _get_item_from_query_row(
                    row=row, request=request, error_on_missing=False
                )
                for row in rows
            ]
        )
        if item is not None
    ]


async def _get_item_from_query_row(
    row: ItemQueryRow, request: Request, error_on_missing: bool = True
) -> Item | None:
    if row.item_content is None:
        try:
            item = Item(
                **StacParser((row.applied_fixes or "").split(",")).parse_stac_item(
                    await _fetch_dict(row.stac_location)
                )[0]
            )
        except UriNotFoundException as e:
            _logger.warning(
                "Item {uri} exists in the index but does not exist in the data store, index is outdated".format(
                    uri=row.stac_location
                )
            )
            if error_on_missing:
                raise NotFoundError(
                    "Item not found in the indexed data store at {uri}. This means the index is outdated, and suggests this item has been removed by the data store and may disappear at the next index update.".format(
                        uri=e.uri,
                    )
                )
            else:
                return None
    else:
        item = Item(**loads(row.item_content))
    return fix_item_links(
        item=item,
        request=request,
    )


async def get_single_collection(collection_id: str, request: Request) -> Collection:
    row = CollectionQueryRow(
        *await fetchone(
            f"SELECT collection_content, stac_location FROM {format_query_object_name('collections')} WHERE id = ?",
            [collection_id],
        )
    )
    if row is None:
        raise NotFoundError(
            "Collection {collection_id} does not exist.".format(
                collection_id=collection_id
            )
        )
    else:
        return cast(
            Collection, await _get_collection_from_query_row(row=row, request=request)
        )  # result cannot be None with error_on_missing as default


async def get_all_collections(request: Request) -> list[Collection]:
    rows = [
        CollectionQueryRow(*row)
        for row in await fetchall(
            f"SELECT collection_content, stac_location FROM {format_query_object_name('collections')} ORDER BY id"
        )
    ]
    return [
        collection
        for collection in await gather(
            *[
                _get_collection_from_query_row(
                    row, request=request, error_on_missing=False
                )
                for row in rows
            ]
        )
        if collection is not None
    ]


async def _get_collection_from_query_row(
    row: CollectionQueryRow, request: Request, error_on_missing: bool = True
) -> Collection | None:
    if row.collection_content is None:
        try:
            collection = Collection(**await _fetch_dict(row[1]))
        except UriNotFoundException as e:
            _logger.warning(
                "Collection {uri} exists in the index but does not exist in the data store, index is outdated".format(
                    uri=row.stac_location
                )
            )
            if error_on_missing:
                raise NotFoundError(
                    "Collection not found in the indexed data store at {uri}. This means the index is outdated, and suggests this collection has been removed by the data store and may disappear at the next index update.".format(
                        uri=e.uri,
                    )
                )
            else:
                return None
    else:
        collection = Collection(**loads(row.collection_content))
    return fix_collection_links(
        collection=collection,
        request=request,
    )


async def _fetch_dict(uri: str) -> Dict[str, Any]:
    async with _semaphore:
        return await get_reader_for_uri(uri=uri).load_json_from_uri(uri)
