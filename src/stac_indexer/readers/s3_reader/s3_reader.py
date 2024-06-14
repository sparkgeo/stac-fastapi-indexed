import re
from concurrent.futures import ThreadPoolExecutor
from json import loads
from logging import Logger, getLogger
from threading import Lock
from typing import Any, Callable, Dict, Final, List, Optional, Tuple, cast

from boto3 import client
from stac_pydantic import Catalog, Collection, Item

from stac_indexer.readers.reader import Reader
from stac_indexer.settings import get_settings
from stac_indexer.types.stac_data import CollectionWithLocation, ItemWithLocation

_url_prefix: Final[str] = r"^s3://(.+)"
_logger: Final[Logger] = getLogger(__file__)
_settings: Final = get_settings()
_item_processor_mutex: Final[Lock] = Lock()


# TODO: some duplication between this and LocalFileReader, needs DRYing
class S3Reader(Reader):
    def __init__(self, root_catalog_url: str):
        self._s3 = client("s3")
        self._provided_root_catalog_url = root_catalog_url

    @classmethod
    def create_reader(cls, url: str) -> Optional[Reader]:
        if re.match(_url_prefix, url):
            return cast(Reader, cls(root_catalog_url=url))
        return None

    def _hasMatchingSelfLink(
        self, with_links: Catalog | Collection | Item, expected_url: str
    ) -> bool:
        for link in with_links.links.link_iterator():
            if link.rel == "self":
                href_match = re.match(_url_prefix, link.href)
                if href_match:
                    return link.href == expected_url
        return False

    def _get_s3_key_parts(self, url: str) -> Tuple[str, str]:
        return cast(re.Match, re.match(r"^s3://([^/]+)/(.+)", url)).groups()

    def _get_json_object(self, url: str) -> Dict[str, Any]:
        try:
            bucket, key = self._get_s3_key_parts(url)
        except Exception as e:
            raise ValueError(f"'{url}' is not in the expected format", e)
        response = (
            self._s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("UTF-8")
        )
        return loads(response)

    def _list_json_objects(self, bucket: str, prefix: str) -> List[str]:
        next_token = None
        all_keys: List[str] = []
        while True:
            list_kwargs = {
                "Bucket": bucket,
                "Prefix": prefix,
            }
            if next_token:
                list_kwargs["ContinuationToken"] = next_token
            response = self._s3.list_objects_v2(**list_kwargs)
            if "Contents" in response:
                for object in response["Contents"]:
                    key = object["Key"]
                    if cast(str, key).endswith(".json"):
                        all_keys.append(key)
            if response.get("IsTruncated"):
                next_token = response.get("NextContinuationToken")
            else:
                break
        return all_keys

    def get_root_catalog(self) -> Catalog:
        _logger.info("reading root catalog")
        dict_catalog = self._get_json_object(self._provided_root_catalog_url)
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
                href_match = re.match(_url_prefix, link.href)
                if href_match:
                    collection_path = link.href
                    _logger.debug(f"reading collection '{collection_path}'")
                    dict_collection = self._get_json_object(collection_path)
                    try:
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
        item_processor: Callable[[ItemWithLocation], List[str]],
    ) -> List[str]:
        _logger.info("reading items for collections")
        all_errors: List[str] = []
        urls: List[str] = []
        for collection in collections:
            _logger.info(f"identifying items for collection '{collection.id}'")
            item_count = 0
            for link in collection.links.link_iterator():
                if link.rel == "items":
                    href_match = re.match(_url_prefix, link.href)
                    if href_match:
                        bucket, prefix = self._get_s3_key_parts(link.href)
                        for key in self._list_json_objects(bucket, prefix):
                            if (
                                _settings.test_collection_item_limit is not None
                                and item_count > _settings.test_collection_item_limit
                            ):
                                _logger.info(
                                    f"exiting item count early due to test limit ({_settings.test_collection_item_limit})"
                                )
                                break
                            urls.append(f"s3://{bucket}/{key}")
                            item_count += 1

        def process_item(url) -> List[str]:
            item_errors = []
            dict_item = self._get_json_object(url)
            try:
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
                        item_processor(ItemWithLocation(**dict_item, location=url))
                    )
            return item_errors

        with ThreadPoolExecutor(max_workers=get_settings().max_threads) as executor:
            all_errors.extend(
                item_errors
                for sublist in executor.map(process_item, urls)
                for item_errors in sublist
            )

        return all_errors
