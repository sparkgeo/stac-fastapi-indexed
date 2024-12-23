from asyncio import gather
from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from re import sub
from typing import Any, Dict, Final, List, Optional, Union, cast

from fastapi import Request
from pygeofilter.ast import Node
from stac_fastapi.extensions.core.filter.filter import FilterExtensionPostRequest
from stac_fastapi.extensions.core.pagination.token_pagination import POSTTokenPagination
from stac_fastapi.extensions.core.sort.sort import SortExtensionPostRequest
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.rfc3339 import str_to_interval
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Item, ItemCollection
from stac_pydantic.api.extensions.sort import SortDirections, SortExtension

from stac_fastapi.indexed.constants import rel_root, rel_self
from stac_fastapi.indexed.db import fetchall
from stac_fastapi.indexed.links.catalog import get_catalog_link
from stac_fastapi.indexed.links.item import fix_item_links
from stac_fastapi.indexed.links.search import get_search_link, get_token_link
from stac_fastapi.indexed.queryables.queryable_field_map import (
    get_queryable_config_by_name,
)
from stac_fastapi.indexed.search.filter.attribute_config import AttributeConfig
from stac_fastapi.indexed.search.filter.errors import (
    NotAGeometryField,
    NotATemporalField,
    UnknownField,
    UnknownFunction,
)
from stac_fastapi.indexed.search.filter.parser import (
    FilterLanguage,
    ast_to_filter_clause,
    filter_to_ast,
    parse_filter_language,
)
from stac_fastapi.indexed.search.filter_clause import FilterClause
from stac_fastapi.indexed.search.query_info import QueryInfo
from stac_fastapi.indexed.search.spatial import (
    get_intersects_clause_for_bbox,
    get_intersects_clause_for_wkt,
)
from stac_fastapi.indexed.search.token import (
    create_token_from_query,
    get_query_from_token,
)
from stac_fastapi.indexed.search.types import SearchDirection, SearchMethod
from stac_fastapi.indexed.sortables.sortable_config import get_sortable_configs_by_field
from stac_fastapi.indexed.stac.fetcher import fetch_dict
from stac_index.common.stac_parser import StacParser

_logger: Final[Logger] = getLogger(__file__)
_text_filter_wrap_key: Final[str] = "__text_filter"

default_sorts: Final[List[SortExtension]] = [
    SortExtension(field="collection", direction=SortDirections.asc),
    SortExtension(field="id", direction=SortDirections.asc),
]


@dataclass
class SearchHandler:
    search_request: BaseSearchPostRequest
    request: Request

    @staticmethod
    def wrap_text_filter(
        filter: str | Dict[str, Any], filter_lang: str
    ) -> Dict[str, Any]:
        # BaseSearchPostRequest only supports dictionary filters.
        # Wrap and unwrap as required, rather than following multiple parsing steps to hack around this.
        filter_type = parse_filter_language(filter_lang)
        if filter_type == FilterLanguage.TEXT:
            return {_text_filter_wrap_key: filter}
        return cast(Dict[str, Any], filter)

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
        rows = fetchall(
            current_query,
            query_info.params,
        )
        has_next_page = len(rows) > query_info.limit
        has_previous_page = query_info.offset is not None
        fetch_tasks = [
            fetch_dict(url) for url in [row[0] for row in rows[0 : query_info.limit]]
        ]
        fixes_to_apply = [
            fix_list.split(",")
            for fix_list in [row[1] for row in rows[0 : query_info.limit]]
        ]
        fetched_dicts = await gather(*fetch_tasks)

        items = [
            fix_item_links(
                Item(**StacParser(fixers).parse_stac_item(item_dict)[1]),
                self.request,
            )
            for (item_dict, fixers) in zip(fetched_dicts, fixes_to_apply)
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
            SELECT stac_location, applied_fixes
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
            effective_sorts = user_provided_sorts
        else:
            effective_sorts = default_sorts
        sortables = get_sortable_configs_by_field()
        for effective_sort in effective_sorts:
            if effective_sort.field not in sortables:
                raise InvalidQueryParameter(
                    f"'{effective_sort.field}' is not sortable, see sortables endpoints"
                )
            sort_fields.append(
                "{} {}".format(
                    sortables[effective_sort.field].items_column,
                    "ASC" if effective_sort.direction == SortDirections.asc else "DESC",
                )
            )
        return sort_fields

    def _include_ids(self) -> Optional[FilterClause]:
        if self.search_request.ids is not None:
            return FilterClause(
                sql="id IN ({})".format(
                    ", ".join(["?" for _ in range(len(self.search_request.ids))])
                ),
                params=self.search_request.ids,
            )
        return None

    def _include_collections(self) -> Optional[FilterClause]:
        if self.search_request.collections is not None:
            return FilterClause(
                sql="collection_id IN ({})".format(
                    ", ".join(
                        ["?" for _ in range(len(self.search_request.collections))]
                    )
                ),
                params=self.search_request.collections,
            )
        return None

    def _include_bbox(self) -> Optional[FilterClause]:
        if self.search_request.bbox is not None:
            bbox_2d = self._get_bbox_2d(self.search_request.bbox)
            if bbox_2d is not None:
                return get_intersects_clause_for_bbox(*bbox_2d)
        return None

    def _include_intersects(self) -> Optional[FilterClause]:
        if self.search_request.intersects is not None:
            return get_intersects_clause_for_wkt(self.search_request.intersects.wkt)
        return None

    def _include_datetime(self) -> Optional[FilterClause]:
        if self.search_request.datetime:
            self.search_request.datetime = str_to_interval(self.search_request.datetime)
            if isinstance(self.search_request.datetime, datetime):
                return FilterClause(
                    sql="""
                    CASE
                        WHEN datetime IS NOT NULL THEN datetime = ?
                        ELSE start_datetime <= ? AND end_datetime >= ?
                    END
                    """,
                    params=[self.search_request.datetime for _ in range(3)],
                )
            elif isinstance(self.search_request.datetime, tuple):
                if (
                    self.search_request.datetime[0] is None
                    and self.search_request.datetime[1] is not None
                ):
                    # ../2000-01-02T00:00:00Z
                    # Start is open, end is not
                    return FilterClause(
                        sql="""
                        CASE
                            WHEN datetime IS NOT NULL THEN datetime <= ?
                            ELSE start_datetime <= ?
                        END
                        """,
                        params=[self.search_request.datetime[1] for _ in range(2)],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request.datetime[1] is None
                ):
                    # 2000-01-01T00:00:00Z/..
                    # End is open, start is not
                    return FilterClause(
                        sql="""
                        CASE
                            WHEN datetime IS NOT NULL THEN datetime >= ?
                            ELSE end_datetime >= ?
                        END
                        """,
                        params=[self.search_request.datetime[0] for _ in range(2)],
                    )
                elif (
                    self.search_request.datetime[0] is not None
                    and self.search_request[1] is not None
                ):
                    # 2000-01-01T00:00:00Z/2000-01-02T00:00:00Z
                    # Neither start or end are open
                    return FilterClause(
                        sql="""
                        CASE
                            WHEN datetime IS NOT NULL THEN datetime >= ? AND datetime <= ?
                            ELSE NOT (end_datetime < ? OR start_datetime > ?)
                        END
                        """,
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

    def _include_filter(self) -> Optional[FilterClause]:
        search_request = cast(FilterExtensionPostRequest, self.search_request)
        if search_request.filter:
            ast = self._get_ast_from_filter(
                search_request.filter, search_request.filter_lang
            )
            queryable_config = get_queryable_config_by_name()
            try:
                return ast_to_filter_clause(
                    ast=ast,
                    attribute_configs=[
                        AttributeConfig(
                            name=entry.name,
                            items_column=entry.items_column,
                            items_column_type=entry.items_column_type,
                            is_geometry=entry.is_geometry,
                            is_temporal=entry.is_temporal,
                        )
                        for entry in queryable_config.values()
                    ],
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
                filter_dict[_text_filter_wrap_key], FilterLanguage.TEXT.value
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
