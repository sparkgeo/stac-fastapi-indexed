import glob
import re
from json import load
from os import path
from typing import Final, List, Optional, Tuple, cast

from stac_pydantic import Catalog, Collection, Item

from stac_indexer import StacData
from stac_indexer.readers.reader import Reader

_url_prefix: Final[str] = r"^file://(.+)"


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
                root_catalog=self._read_root_catalog(),
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

    def _read_root_catalog(self) -> Catalog:
        if path.exists(self.url):
            try:
                with open(self.url, "r") as f:
                    json_catalog = load(f)
            except Exception as e:
                raise Exception(f"Could not read or parse catalog at '{self.url}'", e)
        else:
            raise Exception(f"{self.url} does not exist")
        return Catalog(
            **json_catalog,
        )

    def _read_collections(
        self, root_catalog: Catalog
    ) -> Tuple[List[Collection], List[str]]:
        collections: List[Collection] = []
        errors: List[str] = []
        for link in root_catalog.links.link_iterator():
            if link.rel == "child":
                link_title = link.title or "[untitled]"
                href_match = re.match(_url_prefix, link.href)
                if href_match:
                    collection_path = href_match.group(1)
                    if path.exists(collection_path):
                        try:
                            with open(collection_path, "r") as f:
                                json_collection = load(f)
                            collections.append(
                                Collection(
                                    **json_collection,
                                )
                            )
                        except Exception as e:
                            errors.append(
                                "Could not read or parse collection at '{}': {}".format(
                                    collection_path, e
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
        self, collections: List[Collection]
    ) -> Tuple[List[Item], List[str]]:
        items: List[Item] = []
        errors: List[str] = []
        for collection in collections:
            for link in collection.links.link_iterator():
                if link.rel == "items":
                    href_match = re.match(_url_prefix, link.href)
                    if href_match:
                        items_dir = href_match.group(1)
                        if path.exists(items_dir):
                            for item_path in glob.glob(
                                path.join(items_dir, "**", "*.*json"), recursive=True
                            ):
                                # don't need to check if item_path exists, glob will only return files that exist
                                try:
                                    with open(item_path, "r") as f:
                                        json_item = load(f)
                                    items.append(Item(**json_item))
                                except Exception as e:
                                    errors.append(
                                        "Could not read or parse item at '{}' (collection '{}'): {}".format(
                                            item_path,
                                            collection.id,
                                            e,
                                        )
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
