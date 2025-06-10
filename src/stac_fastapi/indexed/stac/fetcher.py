from typing import Any, Dict, Type

from stac_index.io.readers import SourceReader, get_reader_for_uri
from stac_pydantic.collection import Collection
from stac_pydantic.item import Item

_source_reader_instances: Dict[str, Type[SourceReader]] = {}


async def fetch_dict(uri: str) -> Dict[str, Any]:
    return await get_reader_for_uri(uri=uri).load_json_from_uri(uri)


async def fetch_item(uri: str) -> Item:
    return Item(**(await fetch_dict(uri)))


async def fetch_collection(uri: str) -> Collection:
    return Collection(**(await fetch_dict(uri)))
