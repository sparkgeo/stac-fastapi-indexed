from dataclasses import asdict, dataclass, field
from datetime import datetime
from json import JSONEncoder
from re import escape, match
from typing import Any, Dict, Final, List, Optional, Type, cast

_datetime_field_prefix: Final[str] = "datetime::"


@dataclass(kw_only=True)
class QueryInfo:
    query: str
    params: List[Any] = field(default_factory=list)
    limit: int
    offset: Optional[int] = None

    def next(self) -> "QueryInfo":
        return QueryInfo(
            query=self.query,
            params=self.params,
            limit=self.limit,
            offset=(self.offset + self.limit)
            if self.offset is not None
            else self.limit,
        )

    def previous(self) -> "QueryInfo":
        # Assume logic of validating that a "previous" link is required (i.e. there is currently a non-None offset) is applied elsewhere.
        # Technically we could apply that logic here, but we cannot determine if a "next" link is required in this module, so that would be insconsistent.
        current_offset = cast(int, self.offset)
        return QueryInfo(
            query=self.query,
            params=self.params,
            limit=self.limit,
            offset=(current_offset - self.limit)
            if current_offset > self.limit
            else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def json_encoder() -> Type:
        return _CustomJSONEncoder

    def json_post_decoder(self) -> "QueryInfo":
        new_params = []
        for param in self.params:
            if isinstance(param, str):
                datetime_match = match(rf"^{escape(_datetime_field_prefix)}(.+)", param)
                if datetime_match:
                    new_params.append(datetime.fromisoformat(datetime_match.group(1)))
                    continue
            new_params.append(param)
        return QueryInfo(
            query=self.query,
            params=new_params,
            limit=self.limit,
            offset=self.offset,
        )


class _CustomJSONEncoder(JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return "{}{}".format(
                _datetime_field_prefix,
                obj.isoformat(),
            )
        return JSONEncoder.default(self, obj)
