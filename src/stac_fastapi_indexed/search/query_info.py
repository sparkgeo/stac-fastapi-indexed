from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, cast


@dataclass(kw_only=True)
class QueryInfo:
    query_with_limit_offset_placeholders: str
    params: List[Any] = field(default_factory=list)
    limit: int
    offset: Optional[int] = None

    def next(self) -> "QueryInfo":
        return QueryInfo(
            query_with_limit_offset_placeholders=self.query_with_limit_offset_placeholders,
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
            query_with_limit_offset_placeholders=self.query_with_limit_offset_placeholders,
            params=self.params,
            limit=self.limit,
            offset=(current_offset - self.limit)
            if current_offset > self.limit
            else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
