from dataclasses import dataclass, field
from typing import Any, List


@dataclass
class FilterClause:
    sql: str
    params: List[Any] = field(default_factory=list)
