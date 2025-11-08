"""Offset pagination extension wiring for search request models."""

from typing import Optional

import attr
from fastapi import Query
from pydantic import BaseModel, Field
from typing_extensions import Annotated

from stac_fastapi.extensions.core.pagination.pagination import PaginationExtension
from stac_fastapi.types.search import APIRequest

_offset_description = (
    "Zero-based offset for result set (must be a non-negative integer)."
)

OffsetQuery = Annotated[
    Optional[int],
    Query(
        ge=0,
        description=_offset_description,
    ),
]


@attr.s
class GETOffsetPagination(APIRequest):

    offset: OffsetQuery = attr.ib(default=None)


class POSTOffsetPagination(BaseModel):

    offset: Optional[int] = Field(
        default=None,
        ge=0,
        description=_offset_description,
    )


@attr.s
class OffsetPaginationExtension(PaginationExtension):

    GET = GETOffsetPagination
    POST = POSTOffsetPagination
