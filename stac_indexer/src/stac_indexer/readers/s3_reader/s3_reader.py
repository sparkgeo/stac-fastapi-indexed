import re
from concurrent.futures import ThreadPoolExecutor
from logging import Logger, getLogger
from threading import Lock
from typing import Any, Callable, Dict, Final, List, Optional, Tuple, cast

from boto3 import client
from stac_pydantic import Catalog, Collection, Item

from stac_index_common.data_stores.s3 import (
    get_json_object_from_url,
    get_s3_key_parts,
    list_objects_from_url,
    url_prefix_regex,
)
from stac_indexer.readers.reader import Reader
from stac_indexer.settings import get_settings
from stac_indexer.types.stac_data import CollectionWithLocation, ItemWithLocation

_logger: Final[Logger] = getLogger(__file__)
_settings: Final = get_settings()
_item_processor_mutex: Final[Lock] = Lock()


# TODO: some duplication between this and LocalFileReader, needs DRYing
class S3Reader(Reader):
    def __init__(self, root_catalog_url: str):
        client_args: Dict[str, Any] = {}
        s3_endpoint = get_settings().s3_endpoint
        if s3_endpoint is not None:
            client_args["endpoint_url"] = s3_endpoint
            if s3_endpoint.startswith("http://"):
                client_args["use_ssl"] = False
        self._s3 = client("s3", **client_args)
        self._provided_root_catalog_url = root_catalog_url

    @classmethod
    def create_reader(cls, url: str) -> Optional[Reader]:
        if re.match(url_prefix_regex, url):
            return cast(Reader, cls(root_catalog_url=url))
        return None

    def _hasMatchingSelfLink(
        self, with_links: Catalog | Collection | Item, expected_url: str
    ) -> bool:
        for link in with_links.links.link_iterator():
            if link.rel == "self":
                href_match = re.match(url_prefix_regex, link.href)
                if href_match:
                    return link.href == expected_url
        return False

    def get_root_catalog(self) -> Catalog:
        _logger.info("reading root catalog")
        dict_catalog = get_json_object_from_url(
            self._s3, self._provided_root_catalog_url
        )
        try:
            catalog = Catalog(**dict_catalog)
        except Exception as e:
            raise Exception(
                f"Could not read or parse root catalog at '{self._provided_root_catalog_url}'",
                e,
            )
        if not self._hasMatchingSelfLink(catalog, self._provided_root_catalog_url):
            _logger.warn(
                f"Root catalog self link is incorrect and does not match '{self._provided_root_catalog_url}'"
            )
        return Catalog(**dict_catalog)

    def get_collections(
        self, root_catalog: Catalog
    ) -> Tuple[List[CollectionWithLocation], List[str]]:
        _logger.info("reading collections")
        collections: List[CollectionWithLocation] = []
        errors: List[str] = []
        for link in root_catalog.links.link_iterator():
            if link.rel == "child":
                link_title = link.title or "[untitled]"
                href_match = re.match(url_prefix_regex, link.href)
                if href_match:
                    collection_path = link.href
                    _logger.debug(f"reading collection '{collection_path}'")
                    try:
                        dict_collection = get_json_object_from_url(
                            self._s3, collection_path
                        )
                        collection = Collection(**dict_collection)
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
                            **dict_collection,
                            location=collection_path,
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

    def process_items(
        self,
        collections: List[Collection],
        item_ingestor: Callable[[ItemWithLocation], List[str]],
    ) -> List[str]:
        _logger.info("reading items for collections")
        all_errors: List[str] = []
        item_urls: List[str] = []
        if _settings.test_collection_limit is not None:
            collections = collections[: _settings.test_collection_limit]
        for collection in collections:
            _logger.info(f"identifying items for collection '{collection.id}'")
            item_count = 0
            for link in collection.links.link_iterator():
                if link.rel == "items":
                    href_match = re.match(url_prefix_regex, link.href)
                    if href_match:
                        bucket, _ = get_s3_key_parts(link.href)
                        for key in list_objects_from_url(self._s3, link.href, ".json"):
                            if (
                                _settings.test_collection_item_limit is not None
                                and item_count > _settings.test_collection_item_limit
                            ):
                                _logger.info(
                                    f"exiting item count early due to test limit ({_settings.test_collection_item_limit})"
                                )
                                break
                            item_urls.append(f"s3://{bucket}/{key}")
                            item_count += 1

        def fetch_and_ingest(url) -> List[str]:
            item_errors = []
            try:
                dict_item = get_json_object_from_url(self._s3, url)
                item = Item(**dict_item)
            except Exception as e:
                item_errors.append(
                    "Could not read or parse item at '{}' (collection '{}'): {}".format(
                        url,
                        collection.id,
                        e,
                    )
                )
            else:
                if not self._hasMatchingSelfLink(item, url):
                    _logger.warn(
                        "Item '{}' self link is incorrect and does not match '{}'".format(
                            item.id,
                            url,
                        )
                    )

                # ensure processor does not need to know it is being called
                # from different threads
                with _item_processor_mutex:
                    item_errors.extend(
                        item_ingestor(ItemWithLocation(**dict_item, location=url))
                    )
            return item_errors

        with ThreadPoolExecutor(max_workers=get_settings().max_threads) as executor:
            all_errors.extend(
                item_errors
                for sublist in executor.map(fetch_and_ingest, item_urls)
                for item_errors in sublist
            )

        return all_errors
