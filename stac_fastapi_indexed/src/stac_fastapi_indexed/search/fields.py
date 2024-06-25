from dataclasses import dataclass
from typing import Any, Dict, Final, List, Set

from stac_fastapi.extensions.core.fields.request import PostFieldsExtension
from stac_pydantic.item import Item


@dataclass
class _NoneFallbackField:
    field: str
    fallback: List[str | "_NoneFallbackField"]


_IncludeSet = Set[str | _NoneFallbackField]
_default_includes: Final[_IncludeSet] = set(
    [
        "type",
        "stac_version",
        "id",
        "geometry",
        "bbox",
        "links",
        "assets",
        _NoneFallbackField(
            field="properties.datetime",
            fallback=["properties.start_datetime", "properties.end_datetime"],
        ),
    ]
)


# https://github.com/stac-api-extensions/fields?tab=readme-ov-file#includeexclude-semantics
def limit_item_dict_fields(
    item_dict: Dict[str, Any], requested_fields: PostFieldsExtension
) -> Item:
    include: _IncludeSet = requested_fields.include
    exclude = requested_fields.exclude
    if include is None and exclude is None:
        include = _default_includes
