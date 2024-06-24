from asyncio import gather
from dataclasses import dataclass
from datetime import datetime
from json import loads
from logging import Logger, getLogger
from re import sub
from typing import Any, Dict, Final, List, Optional, Union, cast

from duckdb import DuckDBPyConnection
from fastapi import Request
from pygeofilter.ast import Node
from stac_fastapi.extensions.core.filter.filter import FilterExtensionPostRequest
from stac_fastapi.extensions.core.pagination.token_pagination import POSTTokenPagination
from stac_fastapi.extensions.core.sort.sort import SortExtensionPostRequest
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Item, ItemCollection
from stac_pydantic.api.extensions.sort import SortDirections

from stac_fastapi_indexed.constants import rel_root, rel_self
from stac_fastapi_indexed.fetchers import fetch
from stac_fastapi_indexed.links.catalog import get_catalog_link
from stac_fastapi_indexed.links.item import fix_item_links
from stac_fastapi_indexed.links.search import get_search_link, get_token_link
from stac_fastapi_indexed.search.filter.errors import (
    NotAGeometryField,
    NotATemporalField,
    UnknownField,
    UnknownFunction,
)
from stac_fastapi_indexed.search.filter.parser import (
    FilterLanguages,
    ast_to_search_clause,
    filter_to_ast,
)
from stac_fastapi_indexed.search.filter.queryable_field_map import (
    get_queryable_config_by_name,
)
from stac_fastapi_indexed.search.query_info import QueryInfo
from stac_fastapi_indexed.search.search_clause import SearchClause
from stac_fastapi_indexed.search.token import (
    create_token_from_query,
    get_query_from_token,
)
from stac_fastapi_indexed.search.types import SearchDirection, SearchMethod

_logger: Final[Logger] = getLogger(__file__)
_text_filter_wrap_key: Final[str] = "__text_filter"


@dataclass
class SearchHandler:
    search_request: BaseSearchPostRequest
    request: Request

    @staticmethod
    def wrap_text_filter(filter: str) -> Dict[str, Any]:
        # BaseSearchPostRequest only supports dictionary filters.
        # Wrap and unwrap as required, rather than following multiple parsing steps to hack around this.
        return {_text_filter_wrap_key: filter}

    async def search(self) -> ItemCollection:
        if cast(POSTTokenPagination, self.search_request).token is None:
            _logger.debug("no token, building new query")
            query_info = self._new_query()
        else:
            _logger.debug("token provided")
            query_info = get_query_from_token(
                cast(POSTTokenPagination, self.search_request).token
            )
        limit_text = f"LIMIT {query_info.limit + 1}"  # request one more so that we know if there's a next page of results, extra row is not included in response
        offset_text = (
            f"OFFSET {query_info.offset}" if query_info.offset is not None else ""
        )
        current_query = query_info.query.format(
            limit=limit_text,
            offset=offset_text,
        )
        _logger.debug(f"{current_query}: {query_info.params}")
        rows = (
            cast(DuckDBPyConnection, self.request.app.state.db_connection)
            .execute(
                current_query,
                query_info.params,
            )
            .fetchall()
        )
        has_next_page = len(rows) > query_info.limit
        has_previous_page = query_info.offset is not None
        fetch_tasks = [
            fetch(url) for url in [row[0] for row in rows[0 : query_info.limit]]
        ]
        items = [
            fix_item_links(
                Item(**loads(item_json)),
                self.request,
            )
            for item_json in await gather(*fetch_tasks)
        ]
        links = [
            get_catalog_link(self.request, rel_root),
            get_search_link(self.request, rel_self),
        ]
        if has_next_page:
            links.append(
                get_token_link(
                    self.request,
                    SearchDirection.Next,
                    SearchMethod.from_str(self.request.method),
                    create_token_from_query(query_info.next()),
                )
            )
        if has_previous_page:
            links.append(
                get_token_link(
                    self.request,
                    SearchDirection.Previous,
                    SearchMethod.from_str(self.request.method),
                    create_token_from_query(query_info.previous()),
                )
            )
        return ItemCollection(
            features=items,
            links=links,
        )

    def _new_query(
        self,
    ) -> QueryInfo:
        clauses: List[str] = []
        params: List[Any] = []
        sorts: List[str] = self._determine_order()
        for addition in [
            addition
            for addition in [
                self._include_ids(),
                self._include_collections(),
                self._include_bbox(),
                self._include_intersects(),
                self._include_datetime(),
                self._include_filter(),
            ]
            if addition is not None
        ]:
            clauses.append(addition.sql)
            params.extend(addition.params)
        query = """
            SELECT stac_location
            FROM items
              {where}
              {order}
              {{limit}}
              {{offset}}
            """.format(
            where="WHERE {}".format(" AND ".join(clauses)) if len(clauses) > 0 else "",
            order="ORDER BY {}".format(", ".join(sorts)),
        )
        return QueryInfo(
            query=sub(r"\s+", " ", query).strip(),
            params=params,
            limit=self.search_request.limit,
            offset=None,
        )

    def _determine_order(self) -> List[str]:
        sort_fields: List[str] = []
        user_provided_sorts = cast(SortExtensionPostRequest, self.search_request).sortby
        if user_provided_sorts is not None and len(user_provided_sorts) > 0:
            for user_provided_sort in user_provided_sorts:
                sort_fields.append(
                    "{} {}".format(
                        user_provided_sort.field,
                        "ASC"
                        if user_provided_sort.direction == SortDirections.asc
                        else "DESC",
                    )
                )
        else:
            sort_fields.append("collection_id ASC, id ASC")
        return sort_fields

    def _include_ids(self) -> Optional[SearchClause]:
        if self.search_request.ids is not None:
            return SearchClause(
                sql="id IN ({})".format(
                    ", ".join(["?" for _ in range(len(self.search_request.ids))])
                ),
                params=self.search_request.ids,
            )
        return None

    def _include_collections(self) -> Optional[SearchClause]:
        if self.search_request.collections is not None:
            return SearchClause(
                sql="collection_id IN ({})".format(
                    ", ".join(
                        ["?" for _ in range(len(self.search_request.collections))]
                    )
                ),
                params=self.search_request.collections,
            )
        return None

    def _include_bbox(self) -> Optional[SearchClause]:
        if self.search_request.bbox is not None:
            bbox_2d = self._get_bbox_2d(self.search_request.bbox)
            if bbox_2d is not None:
                return SearchClause(
                    sql="ST_Intersects(ST_GeomFromText('{}'), ST_GeomFromWKB(geometry))".format(
                        "POLYGON (({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))".format(
                            xmin=bbox_2d[0],
                            ymin=bbox_2d[1],
                            xmax=bbox_2d[2],
                            ymax=bbox_2d[3],
                        )
                    )
                )
        return None

    def _include_intersects(self) -> Optional[SearchClause]:
        if self.search_request.intersects is not None:
            return SearchClause(
                sql="ST_Intersects(ST_GeomFromGeoJSON('{}', ST_GeomFromWKB(geometry)))".format(
                    self.search_request.intersects
                )
            )
        return None

    def _include_datetime(self) -> Optional[SearchClause]:
        if self.search_request.datetime:
            if isinstance(self.search_request.datetime, datetime):
                return SearchClause(
                    sql="datetime <= ? AND datetime_end >= ?",
                    params=[self.search_request.datetime for _ in range(3)],
                )
            elif isinstance(self.search_request.datetime, tuple):
                if (
                    self.search_request.datetime[0] is None
                    and self.search_request.datetime[1] is not None
                ):
                    return SearchClause(
                        sql="datetime <= ?",
                        params=[self.search_request.datetime[1]],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request.datetime[1] is None
                ):
                    return SearchClause(
                        sql="datetime >= ?",
                        params=[self.search_request.datetime[0]],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request[1] is not None
                ):
                    return SearchClause(
                        sql="NOT (datetime_end < ? OR datetime > ?)",
                        params=[
                            self.search_request.datetime[0],
                            self.search_request.datetime[1],
                        ],
                    )
                else:
                    pass  # unbounded datetime means all results are valid
        return None

    def _include_filter(self) -> Optional[SearchClause]:
        search_request = cast(FilterExtensionPostRequest, self.search_request)
        if search_request.filter:
            ast = self._get_ast_from_filter(
                search_request.filter, search_request.filter_lang
            )
            queryable_config = get_queryable_config_by_name(
                cast(DuckDBPyConnection, self.request.app.state.db_connection)
            )
            try:
                return ast_to_search_clause(
                    ast=ast,
                    geometry_fields=[
                        key
                        for key, value in queryable_config.items()
                        if value.is_geometry
                    ],
                    temporal_fields=[
                        key
                        for key, value in queryable_config.items()
                        if value.is_temporal
                    ],
                    field_mapping={
                        key: value.items_column
                        for key, value in queryable_config.items()
                    },
                )
            except UnknownField as e:
                raise InvalidQueryParameter(e.field_name)
            except (NotAGeometryField, NotATemporalField) as e:
                raise InvalidQueryParameter(e.argument)
            except UnknownFunction as e:
                raise InvalidQueryParameter(e.function_name)
        return None

    def _get_ast_from_filter(
        self, filter_dict: Dict[str, Any], filter_lang: str
    ) -> Node:
        if _text_filter_wrap_key in filter_dict:
            return filter_to_ast(
                filter_dict[_text_filter_wrap_key], FilterLanguages.TEXT.value
            )
        else:
            return filter_to_ast(filter_dict, filter_lang)

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
