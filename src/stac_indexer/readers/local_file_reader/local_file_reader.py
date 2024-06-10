import glob
import re
from json import load
from logging import Logger, getLogger
from os import path
from typing import Final, List, Optional, Tuple, cast

from stac_pydantic import Catalog, Collection, Item

from stac_indexer.readers.reader import Reader
from stac_indexer.settings import get_settings
from stac_indexer.types.data_access_type import DataAccessType
from stac_indexer.types.stac_data import (
    CatalogWithLocation,
    CollectionWithLocation,
    ItemWithLocation,
    StacData,
)

_url_prefix: Final[str] = r"^file://(.+)"
_logger: Final[Logger] = getLogger(__file__)
_settings: Final = get_settings()


class LocalFileReader(Reader):
    def __init__(self, root_catalog_url):
        self._provided_root_catalog_url = root_catalog_url

    @classmethod
    def create_reader(cls, url: str) -> Optional[Reader]:
        if re.match(_url_prefix, url):
            return cast(Reader, cls(root_catalog_url=url))
        return None

    def process(self) -> Tuple[StacData, List[str]]:
        try:
            root_catalog = self._read_root_catalog()
        except Exception as e:
            raise Exception("Cannot proceed without root catalog", e)

        collections, collections_errors = self._read_collections(
            root_catalog=root_catalog
        )
        items, items_errors = self._read_items(collections=collections)
        return (
            StacData(
                data_access_type=DataAccessType.LOCAL_FILE,
                root_catalog=root_catalog,
                collections=collections,
                items=items,
            ),
            collections_errors + items_errors,
        )

    @property
    def url(self) -> str:
        return cast(
            re.Match[str], re.match(_url_prefix, self._provided_root_catalog_url)
        ).group(1)

    def _hasMatchingSelfLink(
        self, with_links: Catalog | Collection | Item, expected_url: str
    ) -> bool:
        for link in with_links.links.link_iterator():
            if link.rel == "self":
                href_match = re.match(_url_prefix, link.href)
                if href_match:
                    return href_match.group(1) == expected_url
        return False

    def _read_root_catalog(self) -> CatalogWithLocation:
        _logger.info("reading root catalog")
        if path.exists(self.url):
            try:
                with open(self.url, "r") as f:
                    json_catalog = load(f)
                catalog = Catalog(**json_catalog)
            except Exception as e:
                raise Exception(
                    f"Could not read or parse root catalog at '{self.url}'", e
                )
        else:
            raise Exception(f"{self.url} does not exist")
        if not self._hasMatchingSelfLink(catalog, self.url):
            _logger.warn(
                f"Root catalog self link is incorrect and does not match '{self.url}'"
            )
        return CatalogWithLocation(
            **json_catalog,
            location=self.url,
        )

    def _read_collections(
        self, root_catalog: CatalogWithLocation
    ) -> Tuple[List[CollectionWithLocation], List[str]]:
        _logger.info("reading collections")
        collections: List[CollectionWithLocation] = []
        errors: List[str] = []
        for link in root_catalog.links.link_iterator():
            if link.rel == "child":
                link_title = link.title or "[untitled]"
                href_match = re.match(_url_prefix, link.href)
                if href_match:
                    collection_path = href_match.group(1)
                    if path.exists(collection_path):
                        _logger.debug(f"reading collection '{collection_path}'")
                        try:
                            with open(collection_path, "r") as f:
                                json_collection = load(f)
                            collection = Collection(**json_collection)
                        except Exception as e:
                            errors.append(
                                "Could not read or parse collection at '{}': {}".format(
                                    collection_path, e
                                )
                            )
                            continue
                        if not self._hasMatchingSelfLink(collection, collection_path):
                            _logger.warn(
                                "Collection '{}' self link is incorrect and does not match '{}'".format(
                                    collection.id,
                                    collection_path,
                                )
                            )
                        collections.append(
                            CollectionWithLocation(
                                **json_collection,
                                location=collection_path,
                            )
                        )
                    else:
                        errors.append(
                            "Collection '{}' does not exist at '{}'".format(
                                link_title,
                                link.href,
                            )
                        )
                else:
                    errors.append(
                        "Could not process collection '{}' with href '{}'".format(
                            link_title,
                            link.href,
                        )
                    )
        return (collections, errors)

    # TODO: test optimisation with multithreading, reading all these files doesn't need to be serial (post-PoC)
    def _read_items(
        self, collections: List[CollectionWithLocation]
    ) -> Tuple[List[ItemWithLocation], List[str]]:
        _logger.info("reading items for collections")
        items: List[ItemWithLocation] = []
        errors: List[str] = []
        for collection in collections:
            _logger.info(f"reading items for collection '{collection.id}'")
            read_item_count = 0
            for link in collection.links.link_iterator():
                if link.rel == "items":
                    href_match = re.match(_url_prefix, link.href)
                    if href_match:
                        items_dir = href_match.group(1)
                        if path.exists(items_dir):
                            for item_path in glob.glob(
                                path.join(items_dir, "**", "*.*json"), recursive=True
                            ):
                                if (
                                    _settings.test_collection_item_limit is not None
                                    and read_item_count
                                    > _settings.test_collection_item_limit
                                ):
                                    _logger.info(
                                        f"exiting item count early due to test limit ({_settings.test_collection_item_limit})"
                                    )
                                    break
                                # don't need to check if item_path exists, glob will only return files that exist
                                _logger.debug(f"reading item from '{item_path}'")
                                try:
                                    with open(item_path, "r") as f:
                                        json_item = load(f)
                                    item = Item(**json_item)
                                except Exception as e:
                                    errors.append(
                                        "Could not read or parse item at '{}' (collection '{}'): {}".format(
                                            item_path,
                                            collection.id,
                                            e,
                                        )
                                    )
                                    continue
                                finally:
                                    read_item_count += 1
                                if not self._hasMatchingSelfLink(item, item_path):
                                    _logger.warn(
                                        "Item '{}' self link is incorrect and does not match '{}'".format(
                                            item.id,
                                            item_path,
                                        )
                                    )
                                items.append(
                                    ItemWithLocation(**json_item, location=item_path)
                                )
                        else:
                            errors.append(
                                "Items directory '{}' for collection '{}' does not exist".format(
                                    link.href,
                                    collection.id,
                                )
                            )
                    else:
                        errors.append(
                            "Could not process collection '{}' items with href '{}'".format(
                                collection.id,
                                link.href,
                            )
                        )
        return (items, errors)
