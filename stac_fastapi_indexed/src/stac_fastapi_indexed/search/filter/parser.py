from enum import Enum
from typing import Dict, List

from pygeofilter.ast import Node
from stac_fastapi.types.errors import InvalidQueryParameter

from stac_fastapi_indexed.search.filter.duckdb_sql_evaluator import to_search_clause
from stac_fastapi_indexed.search.search_clause import SearchClause


class _TypeParsers(str, Enum):
    JSON2 = "cql2-json"
    JSON = "cql-json"
    CQL = "cql2-text"


def filter_to_ast(filter_lang_name: str, filter: str) -> Node:
    try:
        parser_type = _TypeParsers(filter_lang_name)
    except KeyError:
        raise InvalidQueryParameter(f"Unsupported filter language {filter_lang_name}.")
    if parser_type == _TypeParsers.JSON2:
        from pygeofilter.parsers.cql2_json import parse
    if parser_type == _TypeParsers.JSON:
        from pygeofilter.parsers.cql_json import parse
    elif parser_type == _TypeParsers.CQL:
        from pygeofilter.parsers.ecql import parse
    try:
        return parse(filter)
    except Exception as e:
        raise InvalidQueryParameter(e)


def ast_to_search_clause(
    ast: Node, geometry_fields: List[str], field_mapping: Dict[str, str]
) -> SearchClause:
    return to_search_clause(ast, geometry_fields, field_mapping)
