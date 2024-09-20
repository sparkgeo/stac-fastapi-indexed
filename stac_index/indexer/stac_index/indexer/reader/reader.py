from asyncio import Semaphore, gather
from dataclasses import dataclass
from logging import Logger, getLogger
from re import Pattern, compile, sub
from threading import Lock
from typing import Any, Callable, Dict, Final, List, Protocol, Tuple, cast

from stac_pydantic import Catalog, Collection, Item
from stac_pydantic.links import Link, Links

from stac_index.common import source_reader_classes
from stac_index.common.source_reader import SourceReader
from stac_index.indexer.settings import get_settings
from stac_index.indexer.types.stac_data import CollectionWithLocation, ItemWithLocation

_settings: Final = get_settings()
_logger: Final[Logger] = getLogger(__file__)
_item_processor_mutex: Final[Lock] = Lock()
_link_strip_regex: Final[Pattern] = compile(r"[^/]+$")


class _HasLinks(Protocol):
    links: Links


@dataclass
class Reader:
    root_catalog_uri: str

    def __post_init__(self):
        self._source_reader = None

    # currently assumes only one uri-style for the entire catalog
    def _get_source_reader_for_uri(self) -> SourceReader:
        if self._source_reader is None:
            for reader_class in source_reader_classes:
                if reader_class.can_handle_uri(self.root_catalog_uri):
                    self._source_reader = reader_class()
                    break
        if self._source_reader is None:
            raise Exception(
                f"unable to locate reader capable of reading '{self.root_catalog_uri}'"
            )
        return self._source_reader

    async def _get_json_content_from_uri(self, uri: str) -> Dict[str, Any]:
        return await self._get_source_reader_for_uri().load_json_from_uri(uri)

    async def get_root_catalog(self) -> Catalog:
        _logger.info("reading root catalog")
        dict_catalog = await self._get_json_content_from_uri(self.root_catalog_uri)
        try:
            catalog = Catalog(**dict_catalog)
        except Exception as e:
            raise Exception(
                f"Could not read or parse root catalog at '{self.root_catalog_uri}'",
                e,
            )
        if not self._has_matching_self_link(catalog, self.root_catalog_uri):
            _logger.warn(
                f"Root catalog self link is incorrect and does not match '{self.root_catalog_uri}'"
            )
        catalog = Catalog(**dict_catalog)
        self._expand_relative_links(catalog, self.root_catalog_uri)
        return catalog

    async def get_collections(
        self, root_catalog: Catalog
    ) -> Tuple[List[CollectionWithLocation], List[str]]:
        _logger.info("reading collections")
        collections: List[CollectionWithLocation] = []
        errors: List[str] = []
        for link in root_catalog.links.link_iterator():
            link = cast(Link, link)
            if link.rel == "child":
                collection_path = link.href
                _logger.debug(f"reading collection '{collection_path}'")
                try:
                    dict_collection = await self._get_json_content_from_uri(
                        collection_path
                    )
                    collection = Collection(**dict_collection)
                except Exception as e:
                    errors.append(
                        "Could not read or parse collection at '{}': {}".format(
                            collection_path, e
                        )
                    )
                    continue
                if not self._has_matching_self_link(collection, collection_path):
                    _logger.warn(
                        "Collection '{}' self link is incorrect and does not match '{}'".format(
                            collection.id,
                            collection_path,
                        )
                    )
                collection = CollectionWithLocation(
                    **dict_collection,
                    location=collection_path,
                )
                self._expand_relative_links(collection, collection_path)
                collections.append(collection)
        return (collections, errors)

    async def process_items(
        self,
        collections: List[Collection],
        item_ingestor: Callable[[ItemWithLocation], List[str]],
    ) -> List[str]:
        _logger.info("reading items for collections")
        all_errors: List[str] = []
        item_uris: List[str] = []
        if _settings.test_collection_limit is not None:
            collections = collections[: _settings.test_collection_limit]
        results = await gather(
            *[
                self._get_collection_item_uris(
                    collection, Semaphore(get_settings().max_concurrency)
                )
                for collection in collections
            ]
        )
        for result in results:
            item_uris.extend(result[0])
            all_errors.extend(result[1])

        async def fetch_and_ingest(uri: str, semaphore: Semaphore) -> List[str]:
            async with semaphore:
                _logger.debug(f"fetch_and_ingest {uri}")
                item_errors = []
                try:
                    dict_item = await self._get_json_content_from_uri(uri)
                    item = Item(**dict_item)
                except Exception as e:
                    item_errors.append(
                        "Could not read or parse item at '{}': {}".format(
                            uri,
                            e,
                        )
                    )
                else:
                    if not self._has_matching_self_link(item, uri):
                        _logger.debug(
                            "Item '{}' self link is incorrect and does not match '{}'".format(
                                item.id,
                                uri,
                            )
                        )

                    # ensure item_ingestor cannot be called concurrently by concurrent async function calls
                    with _item_processor_mutex:
                        item_errors.extend(
                            item_ingestor(ItemWithLocation(**dict_item, location=uri))
                        )
                return item_errors

        _logger.info(
            f"starting processing {len(item_uris)} items with max concurrency {get_settings().max_concurrency}"
        )
        items_semaphore = Semaphore(get_settings().max_concurrency)
        all_errors.extend(
            item_errors
            for sublist in await gather(
                *[fetch_and_ingest(uri, items_semaphore) for uri in item_uris]
            )
            for item_errors in sublist
        )
        _logger.info(f"completed processing {len(item_uris)} items")

        return all_errors

    async def _get_collection_item_uris(
        self, collection: Collection, semaphore: Semaphore
    ) -> Tuple[List[str], List[str]]:
        item_uris: List[str] = []
        errors: List[str] = []
        item_single_uris = [
            link.href for link in collection.links.link_iterator() if link.rel == "item"
        ]
        item_uris.extend(
            [href for href in item_single_uris][: _settings.test_collection_item_limit]
        )
        remaining_allowed_links = (
            None
            if _settings.test_collection_item_limit is None
            else _settings.test_collection_item_limit - len(item_uris)
        )
        for items_multiple_uri in [
            link.href
            for link in collection.links.link_iterator()
            if link.rel == "items"
        ]:
            if remaining_allowed_links is None or remaining_allowed_links > 0:
                async with semaphore:
                    (
                        collection_item_uris,
                        collection_item_errors,
                    ) = await self._get_source_reader_for_uri().get_item_uris_from_items_uri(
                        items_multiple_uri, remaining_allowed_links
                    )
                remaining_allowed_links = (
                    None
                    if remaining_allowed_links is None
                    else remaining_allowed_links - len(collection_item_uris)
                )
                item_uris.extend(collection_item_uris)
                errors.extend(collection_item_errors)
        return (item_uris, errors)

    def _has_matching_self_link(
        self, links_provider: _HasLinks, expected_url: str
    ) -> bool:
        for link in links_provider.links.link_iterator():
            if link.rel == "self":
                return link.href == expected_url
        return False

    def _expand_relative_links(
        self, links_provider: _HasLinks, provider_href: str
    ) -> None:
        for link in links_provider.links.link_iterator():
            if link.href.startswith("."):
                _logger.debug(
                    f"expanding relative link '{link.href}' from '{provider_href}'"
                )
                link.href = "{}{}".format(
                    sub(_link_strip_regex, "", provider_href),
                    link.href,
                )
                _logger.debug(f"new link: '{link.href}'")
