from dataclasses import asdict, dataclass, replace
from datetime import datetime
from json import JSONEncoder
from re import escape, match
from typing import Any, Dict, Final, List, Optional, Type, cast

from stac_fastapi.indexed.search.filter_clause import FilterClause

_datetime_field_prefix: Final[str] = "datetime::"

# Increment this value if query structure changes, so that paging tokens from
# older query structures can be rejected.
current_query_version: Final[int] = 1


@dataclass(kw_only=True)
class QueryInfo:
    query_version: int
    ids: Optional[FilterClause] = None
    collections: Optional[FilterClause] = None
    bbox: Optional[FilterClause] = None
    intersects: Optional[FilterClause] = None
    datetime: Optional[FilterClause] = None
    filter: Optional[FilterClause] = None
    order: List[str]
    limit: int
    offset: Optional[int] = None
    last_load_id: str

    @property
    def query_additions(self) -> List[FilterClause]:
        return [
            entry
            for entry in [
                self.ids,
                self.collections,
                self.bbox,
                self.intersects,
                self.datetime,
                self.filter,
            ]
            if entry is not None
        ]

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
        return asdict(self)

    @staticmethod
    def json_encoder() -> Type:
        return _CustomJSONEncoder

    def _param_or_datetime(self, param: Any) -> Any:
        if param is not None and isinstance(param, str):
            datetime_match = match(rf"^{escape(_datetime_field_prefix)}(.+)", param)
            if datetime_match:
                return datetime.fromisoformat(datetime_match.group(1))
        return param

    def json_post_decoder(self) -> "QueryInfo":
        if self.datetime is not None:
            self.datetime.params = [
                self._param_or_datetime(param=param) for param in self.datetime.params
            ]
        if self.filter is not None:
            self.filter.params = [
                self._param_or_datetime(param=param) for param in self.filter.params
            ]
        return self


class _CustomJSONEncoder(JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return "{}{}".format(
                _datetime_field_prefix,
                obj.isoformat(),
            )
        return JSONEncoder.default(self, obj)
