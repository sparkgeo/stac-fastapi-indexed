from enum import Enum
from typing import Any, Dict, List

from pygeofilter.ast import Node
from stac_fastapi.indexed.search.filter.attribute_config import AttributeConfig
from stac_fastapi.indexed.search.filter.duckdb_sql_evaluator import to_filter_clause
from stac_fastapi.indexed.search.filter_clause import FilterClause
from stac_fastapi.types.errors import InvalidQueryParameter


class FilterLanguage(str, Enum):
    JSON2 = "cql2-json"
    JSON = "cql-json"
    TEXT = "cql2-text"


def parse_filter_language(filter_lang: str) -> FilterLanguage:
    try:
        return FilterLanguage(filter_lang)
    except KeyError:
        raise InvalidQueryParameter(f"Unsupported filter language {filter_lang}.")


def filter_to_ast(filter: Dict[str, Any] | str, filter_lang: str) -> Node:
    parser_type = parse_filter_language(filter_lang)
    if parser_type == FilterLanguage.JSON2:
        from pygeofilter.parsers.cql2_json import parse
    if parser_type == FilterLanguage.JSON:
        from pygeofilter.parsers.cql_json import parse
    elif parser_type == FilterLanguage.TEXT:
        from pygeofilter.parsers.cql2_text import parse
    try:
        return parse(filter)
    except Exception as e:
        raise InvalidQueryParameter(e)


def ast_to_filter_clause(
    ast: Node,
    attribute_configs: List[AttributeConfig],
) -> FilterClause:
    return to_filter_clause(ast, attribute_configs)
