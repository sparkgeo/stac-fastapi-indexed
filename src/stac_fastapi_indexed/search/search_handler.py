from asyncio import gather
from dataclasses import dataclass, field
from datetime import datetime
from json import loads
from logging import Logger, getLogger
from re import sub
from typing import Any, Final, List, Optional, Tuple, Union, cast

from duckdb import DuckDBPyConnection
from fastapi import Request
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Item, ItemCollection

from stac_fastapi_indexed.constants import rel_root, rel_self
from stac_fastapi_indexed.fetchers import fetch
from stac_fastapi_indexed.links.catalog import get_catalog_link
from stac_fastapi_indexed.links.item import fix_item_links
from stac_fastapi_indexed.links.search import get_search_link, get_token_link
from stac_fastapi_indexed.search.token import (
    create_token_from_query_with_limit_offset_placeholders,
    get_query_with_limit_offset_placeholders_from_token,
)
from stac_fastapi_indexed.search.types import SearchDirection, SearchMethod

_logger: Final[Logger] = getLogger(__file__)


@dataclass
class _SearchAddition:
    clauses: List[str] = field(default_factory=list)
    params: List[Any] = field(default_factory=list)


@dataclass
class SearchHandler:
    search_request: BaseSearchPostRequest
    request: Request

    async def search(self, token: Optional[str] = None) -> ItemCollection:
        if token is None:
            _logger.debug("no token, building new query")
            query_with_placeholders, params = (
                self._new_query_with_limit_offset_placeholders()
            )
        else:
            _logger.debug("token provided")
            query_with_placeholders, params = (
                get_query_with_limit_offset_placeholders_from_token(token)
            )

        # logic still to figure out here

        debug_token = create_token_from_query_with_limit_offset_placeholders(
            query_with_placeholders, params
        )
        query = query_with_placeholders.format(offset="", limit="LIMIT 10")

        # ... look up

        _logger.debug(f"{query} [{params}]")
        fetch_tasks = [
            fetch(url)
            for url in [
                row[0]
                for row in cast(
                    DuckDBPyConnection, self.request.app.state.db_connection
                )
                .execute(
                    query,
                    params,
                )
                .fetchall()
            ]
        ]

        items = [
            fix_item_links(
                Item(**loads(item_json)),
                self.request,
            )
            for item_json in await gather(*fetch_tasks)
        ]
        return ItemCollection(
            features=items,
            links=[
                get_catalog_link(self.request, rel_root),
                get_search_link(self.request, rel_self),
                get_token_link(
                    self.request, SearchDirection.Next, SearchMethod.POST, debug_token
                ),
                get_token_link(
                    self.request,
                    SearchDirection.Previous,
                    SearchMethod.POST,
                    debug_token,
                ),
            ],
        )

    def _new_query_with_limit_offset_placeholders(
        self,
    ) -> Tuple[
        str,
        List[Any],
    ]:
        clauses: List[str] = []
        params: List[Any] = []
        for addition in [
            addition
            for addition in [
                self._include_ids(),
                self._include_collections(),
                self._include_bbox(),
                self._include_intersects(),
                self._include_datetime(),
            ]
            if addition is not None
        ]:
            clauses.extend(addition.clauses)
            params.extend(addition.params)
        query = """
            SELECT stac_location
            FROM items
              {where} {{offset}} {{limit}}
            """.format(
            where="WHERE {}".format(" AND ".join(clauses)) if len(clauses) > 0 else "",
        )
        return (
            sub(
                r"\s+", " ", query
            ).strip(),  # cut down whitespace for better tokenization
            params,
        )

    def _include_ids(self) -> Optional[_SearchAddition]:
        if self.search_request.ids is not None:
            return _SearchAddition(
                clauses=[
                    "id IN ({})".format(
                        ", ".join(["?" for _ in range(len(self.search_request.ids))])
                    )
                ],
                params=self.search_request.ids,
            )
        return None

    def _include_collections(self) -> Optional[_SearchAddition]:
        if self.search_request.collections is not None:
            return _SearchAddition(
                clauses=[
                    "collection_id IN ({})".format(
                        ", ".join(
                            ["?" for _ in range(len(self.search_request.collections))]
                        )
                    )
                ],
                params=self.search_request.collections,
            )
        return None

    def _include_bbox(self) -> Optional[_SearchAddition]:
        if self.search_request.bbox is not None:
            bbox_2d = self._get_bbox_2d(self.search_request.bbox)
            if bbox_2d is not None:
                return _SearchAddition(
                    clauses=[
                        "ST_Intersects(ST_GeomFromText('{}'), ST_GeomFromWKB(geometry))".format(
                            "POLYGON (({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))".format(
                                xmin=bbox_2d[0],
                                ymin=bbox_2d[1],
                                xmax=bbox_2d[2],
                                ymax=bbox_2d[3],
                            )
                        )
                    ]
                )
        return None

    def _include_intersects(self) -> Optional[_SearchAddition]:
        if self.search_request.intersects is not None:
            return _SearchAddition(
                clauses=[
                    "ST_Intersects(ST_GeomFromGeoJSON('{}', ST_GeomFromWKB(geometry)))".format(
                        self.search_request.intersects
                    )
                ]
            )
        return None

    def _include_datetime(self) -> Optional[_SearchAddition]:
        if self.search_request.datetime:
            if isinstance(self.search_request.datetime, datetime):
                return _SearchAddition(
                    clauses=[
                        """
                        CASE
                            WHEN datetime_end IS NULL THEN datetime = ?
                            ELSE datetime <= ? AND datetime_end >= ?
                        END
                        """
                    ],
                    params=[self.search_request.datetime for _ in range(3)],
                )
            elif isinstance(self.search_request.datetime, tuple):
                if (
                    self.search_request.datetime[0] is None
                    and self.search_request.datetime[1] is not None
                ):
                    return _SearchAddition(
                        clauses=["datetime <= ?"],
                        params=[self.search_request.datetime[1]],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request.datetime[1] is None
                ):
                    return _SearchAddition(
                        clauses=["datetime >= ?"],
                        params=[self.search_request.datetime[0]],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request[1] is not None
                ):
                    return _SearchAddition(
                        clauses=[
                            """
                            CASE
                                WHEN datetime_end IS NULL THEN datetime >= ? and datetime <= ?
                                ELSE datetime_end >= ? AND datetime <= ?
                            END
                        """
                        ],
                        params=[
                            self.search_request.datetime[0],
                            self.search_request.datetime[1],
                            self.search_request.datetime[0],
                            self.search_request.datetime[1],
                        ],
                    )
                else:
                    pass  # unbounded datetime means all results are valid
        return None

    def _get_bbox_2d(
        self, bbox: List[Union[float, int]]
    ) -> Optional[List[Union[float, int]]]:
        if len(bbox) == 4:
            return bbox
        elif len(bbox) == 6:
            return [bbox[0], bbox[1], bbox[3], bbox[4]]
        else:
            _logger.info(f"unhandled bbox parameter in search request: {bbox}")
            return None
