from dataclasses import dataclass, field
from typing import Any, List


@dataclass
class SearchClause:
    sql: str
    params: List[Any] = field(default_factory=list)
