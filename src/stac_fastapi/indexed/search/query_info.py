from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, Final, List, Optional, cast

from geojson_pydantic.geometries import parse_geometry_obj
from stac_pydantic.api.extensions.sort import SortExtension
from stac_pydantic.api.search import Intersection
from stac_pydantic.shared import BBox

# Increment this value if query structure changes, so that paging tokens from
# older query structures can be rejected.
current_query_version: Final[int] = 1


@dataclass(kw_only=True)
class QueryInfo:
    query_version: int
    ids: Optional[List[str]] = None
    collections: Optional[List[str]] = None
    bbox: Optional[BBox] = None
    intersects: Optional[Intersection] = None
    datetime: Optional[str] = None
    filter: Optional[Dict[str, Any]] = None
    filter_lang: str
    order: Optional[List[SortExtension]] = None
    limit: int
    offset: Optional[int] = None
    last_load_id: str

    def next(self) -> "QueryInfo":
        return replace(
            self,
            offset=(self.offset + self.limit)
            if self.offset is not None
            else self.limit,
        )

    def previous(self) -> "QueryInfo":
        # Assume logic of validating that a "previous" link is required (i.e. there is currently a non-None offset) is applied elsewhere.
        # Technically we could apply that logic here, but we cannot determine if a "next" link is required in this module, so that would be insconsistent.
        current_offset = cast(int, self.offset)
        return replace(
            self,
            offset=(current_offset - self.limit)
            if current_offset > self.limit
            else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "intersects": cast(Intersection, self.intersects).model_dump()
            if self.intersects is not None
            else None,
            "order": [
                {
                    **cast(SortExtension, entry).model_dump(),
                    **{"direction": cast(SortExtension, entry).direction.value},
                }
                for entry in self.order
            ]
            if self.order is not None
            else None,
        }

    @classmethod
    def from_dict(cls, source_dict: Dict[str, Any]) -> "QueryInfo":
        intersects_value = source_dict.get("intersects")
        order_value = source_dict.get("order")
        return cls(
            **{
                **source_dict,
                "intersects": parse_geometry_obj(intersects_value)
                if intersects_value is not None
                else None,
                "order": [
                    SortExtension(**cast(Dict[str, Any], entry))
                    for entry in order_value
                ]
                if order_value is not None
                else None,
            }
        )
