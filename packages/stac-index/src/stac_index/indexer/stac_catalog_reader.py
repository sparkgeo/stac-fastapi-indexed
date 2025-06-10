from asyncio import Semaphore, gather
from dataclasses import dataclass
from logging import Logger, getLogger
from re import Pattern, compile, sub
from threading import Lock
from typing import Any, Callable, Dict, Final, List, Protocol, Tuple, Type, cast

from stac_index.indexer.settings import get_settings
from stac_index.indexer.stac_parser import StacParser, StacParserException
from stac_index.indexer.types.indexing_error import (
    IndexingError,
    IndexingErrorType,
    new_error,
)
from stac_index.indexer.types.stac_data import CollectionWithLocation, ItemWithLocation
from stac_index.io.readers import source_reader_classes
from stac_index.io.readers.source_reader import SourceReader
from stac_pydantic import Catalog, Collection
from stac_pydantic.links import Links


class _HasLinks(Protocol):
    links: Links


_settings: Final = get_settings()
_logger: Final[Logger] = getLogger(__name__)
_item_processor_mutex: Final[Lock] = Lock()
_link_strip_regex: Final[Pattern] = compile(r"[^/]+$")
_child_types_by_lower_type: Final[Dict[str, Type[_HasLinks]]] = {
    "catalog": Catalog,
    "collection": Collection,
}


@dataclass
class StacCatalogReader:
    root_catalog_uri: str
    fixes_to_apply: List[str]

    def __post_init__(self):
        self._source_reader = None
        self._stac_parser = StacParser(self.fixes_to_apply)

    # currently assumes only one uri-style for the entire catalog
    def _get_source_reader_for_uri(self) -> SourceReader:
        if self._source_reader is None:
            for reader_class in source_reader_classes:
                if reader_class.can_handle_uri(self.root_catalog_uri):
                    self._source_reader = reader_class(
                        concurrency=get_settings().max_concurrency
                    )
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
        if not _has_matching_self_link(catalog, self.root_catalog_uri):
            _logger.warn(
                f"Root catalog self link is incorrect and does not match '{self.root_catalog_uri}'"
            )
        catalog = Catalog(**dict_catalog)
        _expand_relative_links(catalog, self.root_catalog_uri)
        return catalog

    async def get_collections(
        self, root_catalog: Catalog
    ) -> Tuple[List[CollectionWithLocation], List[IndexingError]]:
        _logger.info("reading collections")
        collections: List[CollectionWithLocation] = []
        errors: List[IndexingError] = []

        child_links_all: List[str] = [
            link.href
            for link in root_catalog.links.link_iterator()
            if link.rel == "child"
        ]
        child_links_followed: List[str] = []

        # child_links_all can grow as nested catalogs or collections are discovered.
        # Repeatedly process the list until no more links are added.
        while len(set(child_links_all).difference(set(child_links_followed))) > 0:
            for child_link in set(child_links_all).difference(
                set(child_links_followed)
            ):
                child_links_followed.append(child_link)
                try:
                    child_dict = await self._get_json_content_from_uri(child_link)
                except Exception as e:
                    errors.append(
                        new_error(
                            IndexingErrorType.collection_parsing,
                            "Could not read or parse child JSON at '{}': {}".format(
                                child_link, e
                            ),
                        )
                    )
                    continue
                child_type = str(child_dict.get("type", "_unknown_")).lower()
                if child_type not in _child_types_by_lower_type:
                    errors.append(
                        new_error(
                            IndexingErrorType.collection_parsing,
                            "Did not recognise child type at '{}': {}".format(
                                child_link, child_type
                            ),
                        )
                    )
                    continue
                try:
                    child = _child_types_by_lower_type[child_type](**child_dict)
                except Exception as e:
                    errors.append(
                        new_error(
                            IndexingErrorType.collection_parsing,
                            "Could not parse child dictionary as '{}' at '{}': {}".format(
                                child_type, child_link, e
                            ),
                        )
                    )
                    continue
                _expand_relative_links(child, child_link)
                childs_child_links = [
                    link.href
                    for link in cast(_HasLinks, child).links.link_iterator()
                    if link.rel == "child"
                ]
                if len(childs_child_links) > 0:
                    _logger.info(
                        f"Child of type '{child_type}' at '{child_link}' adds {len(childs_child_links)} child(ren)"
                    )
                    child_links_all.extend(childs_child_links)
                if isinstance(child, Collection):
                    collection = cast(Collection, child)
                    if not _has_matching_self_link(collection, child_link):
                        _logger.debug(
                            "Collection '{}' self link is incorrect and does not match '{}'".format(
                                collection.id,
                                child_link,
                            )
                        )
                    collections.append(
                        collection.model_copy(update={"location": child_link})
                    )
        return (collections, errors)

    async def process_items(
        self,
        collections: List[Collection],
        item_ingestor: Callable[[ItemWithLocation], List[IndexingError]],
    ) -> List[IndexingError]:
        _logger.info("reading items for collections")
        all_errors: List[IndexingError] = []
        item_uris: List[str] = []
        if _settings.test_collection_limit is not None:
            collections = collections[: _settings.test_collection_limit]
        _logger.info("collecting item URIs for collections")
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

        collected_item_count = len(item_uris)
        item_uris = list(set(item_uris))
        if len(item_uris) < collected_item_count:
            _logger.info(
                "removed {} duplicate item URIs".format(
                    collected_item_count - len(item_uris)
                )
            )

        async def fetch_and_ingest(
            uri: str, semaphore: Semaphore
        ) -> List[IndexingError]:
            async with semaphore:
                _logger.debug(f"fetch_and_ingest {uri}")
                item_errors: List[IndexingError] = []
                try:
                    dict_item = await self._get_json_content_from_uri(uri)
                    (item, dict_item) = self._stac_parser.parse_stac_item(dict_item)
                except StacParserException as e:
                    item_errors.extend(e.indexing_errors)
                else:
                    if not _has_matching_self_link(item, uri):
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
        all_errors.extend(
            item_errors
            for sublist in await gather(
                *[
                    fetch_and_ingest(uri, Semaphore(get_settings().max_concurrency))
                    for uri in item_uris
                ]
            )
            for item_errors in sublist
        )

        return all_errors

    async def _get_collection_item_uris(
        self, collection: Collection, semaphore: Semaphore
    ) -> Tuple[List[str], List[IndexingError]]:
        item_uris: List[str] = []
        errors: List[IndexingError] = []
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
                errors.extend(
                    [
                        new_error(IndexingErrorType.item_fetching, error)
                        for error in collection_item_errors
                    ]
                )
        return (item_uris, errors)


def _has_matching_self_link(links_provider: _HasLinks, expected_url: str) -> bool:
    for link in links_provider.links.link_iterator():
        if link.rel == "self":
            return link.href == expected_url
    return False


def _expand_relative_links(links_provider: _HasLinks, provider_href: str) -> None:
    for link in links_provider.links.link_iterator():
        if link.href.startswith("."):
            original_relative_link = link.href
            base_link = sub(_link_strip_regex, "", provider_href)
            naive_link = "{}{}".format(
                base_link,
                link.href,
            )
            naive_link_parts = naive_link.split("/")
            new_link_parts: List[str] = []
            for part in naive_link_parts:
                if part == ".":
                    continue
                elif part == "..":
                    new_link_parts.pop()
                else:
                    new_link_parts.append(part)
            link.href = "/".join(new_link_parts)
            _logger.debug(f"expanded '{original_relative_link}' to '{link.href}'")
