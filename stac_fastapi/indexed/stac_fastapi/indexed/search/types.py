from enum import Enum
from typing import Optional

import attr
from stac_fastapi.types.search import (
    APIRequest,
    BaseSearchGetRequest,
    BaseSearchPostRequest,
    _bbox_converter,
    str2list,
)
from stac_pydantic.shared import BBox


def _bbox_converter_safe(val: Optional[str]) -> Optional[BBox]:
    if val is not None:
        parts = str2list(val)
        if len(parts) == 6:
            return _bbox_converter(",".join([parts[0], parts[1], parts[3], parts[4]]))
        else:
            return _bbox_converter(val)
    return None


class SearchDirection(Enum):
    Next = 0
    Previous = 1


class SearchMethod(str, Enum):
    GET = "GET"
    POST = "POST"

    @classmethod
    def from_str(cls, method: str):
        for member in cls:
            if member.value == method.upper():
                return member
        raise ValueError(f"{method} is not a valid {cls.__name__}")


@attr.s
class SearchGetRequest(BaseSearchGetRequest):
    bbox: Optional[BBox] = attr.ib(default=None, converter=_bbox_converter_safe)
    datetime: Optional[str] = attr.ib(default=None)


class SearchPostRequest(BaseSearchPostRequest):
    pass


@attr.s
class BBOX3DCompatible(APIRequest):
    bbox: Optional[BBox] = attr.ib(default=None, converter=_bbox_converter_safe)
