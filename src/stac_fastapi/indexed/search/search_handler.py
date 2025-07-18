from asyncio import gather
from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from typing import Any, Dict, Final, List, Optional, Self, cast

from fastapi import HTTPException, Request, status
from pygeofilter.ast import Node
from stac_fastapi.extensions.core.filter.filter import FilterExtensionPostRequest
from stac_fastapi.extensions.core.pagination.token_pagination import POSTTokenPagination
from stac_fastapi.extensions.core.sort.sort import SortExtensionPostRequest
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.rfc3339 import str_to_interval
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Item, ItemCollection
from stac_index.indexer.stac_parser import StacParser
from stac_index.io.readers.exceptions import UriNotFoundException
from stac_pydantic.api.extensions.sort import SortDirections, SortExtension
from stac_pydantic.api.search import Intersection
from stac_pydantic.shared import BBox

from stac_fastapi.indexed.constants import collection_wildcard, rel_root, rel_self
from stac_fastapi.indexed.db import fetchall, format_query_object_name, get_last_load_id
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
from stac_fastapi.indexed.search.query_info import QueryInfo, current_query_version
from stac_fastapi.indexed.search.spatial import (
    get_intersects_clause_for_bbox,
    get_intersects_clause_for_wkt,
)
from stac_fastapi.indexed.search.token import (
    create_token_from_query,
    get_query_info_from_token,
)
from stac_fastapi.indexed.search.types import SearchDirection, SearchMethod
from stac_fastapi.indexed.sortables.sortable_config import get_sortable_configs_by_field
from stac_fastapi.indexed.stac.fetcher import fetch_dict

_logger: Final[Logger] = getLogger(__name__)
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
        reject_if_load_id_changed = False
        if cast(POSTTokenPagination, self.search_request).token is None:
            _logger.debug("no token, building new query")
            query_info = await self._new_query_info()
        else:
            _logger.debug("token provided")
            # do not permit paging across data changes as paged results may be inconsistent
            reject_if_load_id_changed = True
            query_info = get_query_info_from_token(
                cast(POSTTokenPagination, self.search_request).token
            )
        clauses: List[str] = []
        params: List[Any] = []
        for addition in [
            self._include_ids(ids=query_info.ids),
            self._include_collections(collections=query_info.collections),
            self._include_bbox(bbox=query_info.bbox),
            self._include_intersects(intersects=query_info.intersects),
            self._include_datetime(datetime_str=query_info.datetime),
            await self._include_filter(
                filter_lang=query_info.filter_lang,
                filter=query_info.filter,
                collections=query_info.collections,
            ),
        ]:
            if addition is not None:
                clauses.append(addition.sql)
                params.extend(addition.params)
        query = """
            SELECT stac_location, applied_fixes
            FROM {table_name}
              {where}
              ORDER BY {order}
              LIMIT ?
              OFFSET ?
            """.format(
            table_name=format_query_object_name("items"),
            where="WHERE {}".format(" AND ".join(clauses)) if len(clauses) > 0 else "",
            order=(", ".join(await self._determine_order(query_info.order))),
        )
        params.append(
            query_info.limit + 1
        )  # request one more so that we know if there's a next page of results
        params.append(query_info.offset if query_info.offset is not None else 0)
        rows = await fetchall(
            query,
            params,
        )
        if reject_if_load_id_changed and get_last_load_id() != query_info.last_load_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="STAC data recently changed and paging behaviour cannot be guaranteed. Remove the paging token to start again.",
            )
        has_next_page = len(rows) > query_info.limit
        has_previous_page = query_info.offset is not None

        async def get_each_item(uri: str) -> Optional[Dict[str, Any]]:
            try:
                return await fetch_dict(uri=uri)
            except UriNotFoundException:
                _logger.warning(
                    "Item '{uri}' exists in the index but does not exist in the data store, index is outdated".format(
                        uri=uri
                    )
                )
                return None

        fetch_tasks = [
            get_each_item(url) for url in [row[0] for row in rows[0 : query_info.limit]]
        ]
        fetched_dicts = []
        missing_entry_indices = []
        for i, entry in enumerate(await gather(*fetch_tasks)):
            if entry is None:
                missing_entry_indices.append(i)
            else:
                fetched_dicts.append(entry)
        fixes_to_apply = [
            fix_list.split(",")
            for fix_list in [
                row[1]
                for i, row in enumerate(rows[0 : query_info.limit])
                if i not in missing_entry_indices
            ]
        ]
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
            type="FeatureCollection",
            features=items,
            links=links,
        )

    async def _new_query_info(
        self,
    ) -> QueryInfo:
        return QueryInfo(
            query_version=current_query_version,
            ids=self.search_request.ids,
            collections=self.search_request.collections,
            bbox=self.search_request.bbox,
            intersects=self.search_request.intersects,
            datetime=self.search_request.datetime,
            filter=cast(FilterExtensionPostRequest, self.search_request).filter,
            filter_lang=cast(
                str, cast(FilterExtensionPostRequest, self.search_request).filter_lang
            ),
            order=cast(SortExtensionPostRequest, self.search_request).sortby,
            limit=cast(
                int, self.search_request.limit
            ),  # will have default value if not provided by caller
            offset=None,
            last_load_id=get_last_load_id(),
        )

    async def _determine_order(
        self, sortby: Optional[List[SortExtension]] = None
    ) -> List[str]:
        # DuckDB does not support parameters in ORDER BY statements, so we must use string concatenation
        # to support user-provided sorts. This introductes the risk of SQL injection, however this risk
        # is mitigated by checking provided sort fields against configured sortable fields.
        # A caller cannot provide a sort column identifier that has not been configured as sortable, and
        # therefore should not be able to interfere with query parsing.
        sort_fields: List[str] = []
        if sortby is not None and len(sortby) > 0:
            effective_sorts = sortby
        else:
            effective_sorts = default_sorts

        sortables = await get_sortable_configs_by_field()
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

    def _include_ids(
        self: Self, ids: Optional[List[str]] = None
    ) -> Optional[FilterClause]:
        if ids is not None:
            return FilterClause(
                sql="id IN ({})".format(", ".join(["?" for _ in range(len(ids))])),
                params=ids,
            )
        return None

    def _include_collections(
        self: Self, collections: Optional[List[str]] = None
    ) -> Optional[FilterClause]:
        if collections is not None:
            return FilterClause(
                sql="collection_id IN ({})".format(
                    ", ".join(["?" for _ in range(len(collections))])
                ),
                params=collections,
            )
        return None

    def _include_bbox(
        self: Self, bbox: Optional[BBox] = None
    ) -> Optional[FilterClause]:
        if bbox is not None:
            bbox_2d = self._get_bbox_2d(bbox)
            if bbox_2d is not None:
                return get_intersects_clause_for_bbox(*bbox_2d)
        return None

    def _include_intersects(
        self: Self, intersects: Optional[Intersection] = None
    ) -> Optional[FilterClause]:
        if intersects is not None:
            return get_intersects_clause_for_wkt(intersects.wkt)
        return None

    def _include_datetime(
        self: Self, datetime_str: Optional[str] = None
    ) -> Optional[FilterClause]:
        if datetime_str:
            datetime_arg = str_to_interval(datetime_str)
            if isinstance(datetime_arg, datetime):
                return FilterClause(
                    sql="""
                    CASE
                        WHEN datetime IS NOT NULL THEN datetime = ?
                        ELSE start_datetime <= ? AND end_datetime >= ?
                    END
                    """,
                    params=[datetime_arg for _ in range(3)],
                )
            elif isinstance(datetime_arg, tuple):
                if datetime_arg[0] is None and datetime_arg[1] is not None:
                    # ../2000-01-02T00:00:00Z
                    # Start is open, end is not
                    return FilterClause(
                        sql="""
                        CASE
                            WHEN datetime IS NOT NULL THEN datetime <= ?
                            ELSE start_datetime <= ?
                        END
                        """,
                        params=[datetime_arg[1] for _ in range(2)],
                    )
                elif datetime_arg[0] is not None and datetime_arg[1] is None:
                    # 2000-01-01T00:00:00Z/..
                    # End is open, start is not
                    return FilterClause(
                        sql="""
                        CASE
                            WHEN datetime IS NOT NULL THEN datetime >= ?
                            ELSE end_datetime >= ?
                        END
                        """,
                        params=[datetime_arg[0] for _ in range(2)],
                    )
                elif datetime_arg[0] is not None and datetime_arg[1] is not None:
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
                            datetime_arg[0],
                            datetime_arg[1],
                            datetime_arg[0],
                            datetime_arg[1],
                        ],
                    )
                else:
                    pass  # unbounded datetime means all results are valid
        return None

    async def _include_filter(
        self: Self,
        filter_lang: str,
        filter: Optional[Dict[str, Any]] = None,
        collections: Optional[List[str]] = None,
    ) -> Optional[FilterClause]:
        if filter:
            ast = self._get_ast_from_filter(filter, filter_lang)
            queryable_config = await get_queryable_config_by_name()
            collection_ids_for_queryables = collections or []
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
                        if len(collection_ids_for_queryables) == 0
                        or entry.collection_id == collection_wildcard
                        or entry.collection_id in collection_ids_for_queryables
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

    def _get_bbox_2d(self, bbox: BBox) -> Optional[BBox]:
        if len(bbox) == 4:
            return bbox
        elif len(bbox) == 6:
            return (bbox[0], bbox[1], bbox[3], bbox[4])
        else:
            _logger.info(f"unhandled bbox parameter in search request: {bbox}")
            return None
