from typing import Any, Dict, Optional

from stac_index.common.source_reader import SourceReader
from stac_index.readers import source_reader_classes
from stac_pydantic.collection import Collection
from stac_pydantic.item import Item

_source_reader: Optional[SourceReader] = None


# currently assumes only one uri-style for the entire catalog
def _get_source_reader_for_uri(uri: str) -> SourceReader:
    global _source_reader
    if _source_reader is None:
        for reader_class in source_reader_classes:
            if reader_class.can_handle_uri(uri):
                _source_reader = reader_class()
                break
    if _source_reader is None:
        raise Exception(f"unable to locate reader capable of reading '{uri}'")
    return _source_reader


async def fetch_dict(uri: str) -> Dict[str, Any]:
    return await _get_source_reader_for_uri(uri).load_json_from_uri(uri)


async def fetch_item(uri: str) -> Item:
    return Item(**(await fetch_dict(uri)))


async def fetch_collection(uri: str) -> Collection:
    return Collection(**(await fetch_dict(uri)))
