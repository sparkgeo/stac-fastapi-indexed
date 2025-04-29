from typing import Any, Dict, Type

from stac_index.readers import SourceReader, get_reader_class_for_uri
from stac_pydantic.collection import Collection
from stac_pydantic.item import Item

_source_reader_instances: Dict[str, Type[SourceReader]] = {}


async def fetch_dict(uri: str) -> Dict[str, Any]:
    source_reader_class = get_reader_class_for_uri(uri)
    if source_reader_class.__name__ not in _source_reader_instances:
        _source_reader_instances[source_reader_class.__name__] = source_reader_class()
    source_reader = _source_reader_instances[source_reader_class.__name__]
    return await source_reader.load_json_from_uri(uri)


async def fetch_item(uri: str) -> Item:
    return Item(**(await fetch_dict(uri)))


async def fetch_collection(uri: str) -> Collection:
    return Collection(**(await fetch_dict(uri)))
