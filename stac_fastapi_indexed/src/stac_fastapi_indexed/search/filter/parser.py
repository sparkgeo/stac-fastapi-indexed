from enum import Enum
from typing import Any, Dict, List

from pygeofilter.ast import Node
from stac_fastapi.types.errors import InvalidQueryParameter

from stac_fastapi_indexed.search.filter.duckdb_sql_evaluator import to_search_clause
from stac_fastapi_indexed.search.search_clause import SearchClause


class FilterLanguages(str, Enum):
    JSON2 = "cql2-json"
    JSON = "cql-json"
    TEXT = "cql2-text"


def parse_filter_language(filter_lang: str) -> FilterLanguages:
    try:
        return FilterLanguages(filter_lang)
    except KeyError:
        raise InvalidQueryParameter(f"Unsupported filter language {filter_lang}.")


def filter_to_ast(filter: Dict[str, Any] | str, filter_lang: str) -> Node:
    parser_type = parse_filter_language(filter_lang)
    if parser_type == FilterLanguages.JSON2:
        from pygeofilter.parsers.cql2_json import parse
    if parser_type == FilterLanguages.JSON:
        from pygeofilter.parsers.cql_json import parse
    elif parser_type == FilterLanguages.TEXT:
        from pygeofilter.parsers.cql2_text import parse
    try:
        return parse(filter)
    except Exception as e:
        raise InvalidQueryParameter(e)


def ast_to_search_clause(
    ast: Node,
    geometry_fields: List[str],
    temporal_fields: List[str],
    field_mapping: Dict[str, str],
) -> SearchClause:
    return to_search_clause(ast, geometry_fields, temporal_fields, field_mapping)
