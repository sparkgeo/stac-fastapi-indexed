from typing import Optional

import attr
from stac_fastapi.types.search import BaseSearchGetRequest


@attr.s
class SearchGetRequest(BaseSearchGetRequest):
    datetime: Optional[str] = attr.ib(default=None)
