# ------------------------------------------------------------------------------
#
# Project: pygeofilter <https://github.com/geopython/pygeofilter>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
# ------------------------------------------------------------------------------
# Copyright (C) 2021 EOX IT Services GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies of this Software or works derived from this Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# ------------------------------------------------------------------------------

# Parameterised SQL evaluator derived from https://github.com/geopython/pygeofilter/blob/545f002accb3171727c387dfce5c98e7ca45b13c/pygeofilter/backends/sql/evaluate.py
# Initially developed for DuckDB 1.0.0, but may be suitable to other DBMSs also.
# Pygeofilter's SQL evaluator targets OGR SQL, which does not support parameterised queries, and so does not support parameterisation.
# Non-parameterised SQL is a security concern that this evaluator is intended to address.

from dataclasses import dataclass
from typing import Any, Dict, Final, List, Optional, Tuple, cast

from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle
from shapely import geometry

from stac_fastapi.indexed.search.filter.attribute_config import AttributeConfig
from stac_fastapi.indexed.search.filter.errors import (
    NotAGeometryField,
    NotATemporalField,
    UnknownField,
    UnknownFunction,
)
from stac_fastapi.indexed.search.filter_clause import FilterClause
from stac_fastapi.indexed.search.spatial import get_intersects_clause_for_bbox

_COMPARISON_OP_MAP: Final[Dict[ast.ComparisonOp, str]] = {
    ast.ComparisonOp.EQ: "=",
    ast.ComparisonOp.NE: "<>",
    ast.ComparisonOp.LT: "<",
    ast.ComparisonOp.LE: "<=",
    ast.ComparisonOp.GT: ">",
    ast.ComparisonOp.GE: ">=",
}


_ARITHMETIC_OP_MAP: Final[Dict[ast.ArithmeticOp, str]] = {
    ast.ArithmeticOp.ADD: "+",
    ast.ArithmeticOp.SUB: "-",
    ast.ArithmeticOp.MUL: "*",
    ast.ArithmeticOp.DIV: "/",
}

_SPATIAL_COMPARISON_OP_MAP: Final[Dict[ast.SpatialComparisonOp, str]] = {
    ast.SpatialComparisonOp.INTERSECTS: "ST_Intersects",
    ast.SpatialComparisonOp.DISJOINT: "ST_Disjoint",
    ast.SpatialComparisonOp.CONTAINS: "ST_Contains",
    ast.SpatialComparisonOp.WITHIN: "ST_Within",
    ast.SpatialComparisonOp.TOUCHES: "ST_Touches",
    ast.SpatialComparisonOp.CROSSES: "ST_Crosses",
    ast.SpatialComparisonOp.OVERLAPS: "ST_Overlaps",
    ast.SpatialComparisonOp.EQUALS: "ST_Equals",
}

_TEMPORAL_POINT_COMPARISON_TYPES: Final[List[ast.TemporalPredicate]] = [
    ast.TimeBefore,
    ast.TimeAfter,
    ast.TimeMeets,
    ast.TimeMetBy,
    ast.TimeOverlaps,
    ast.TimeEquals,
]
_TEMPORAL_POINT_COMPARISON_OP_MAP: Final[Dict[ast.TemporalComparisonOp, str]] = {
    ast.TemporalComparisonOp.BEFORE: "{interval_end} < {point_comparator}",
    ast.TemporalComparisonOp.AFTER: "{interval_start} > {point_comparator}",
    ast.TemporalComparisonOp.MEETS: "{interval_end} = {point_comparator}",
    ast.TemporalComparisonOp.METBY: "{interval_start} = {point_comparator}",
    ast.TemporalComparisonOp.TOVERLAPS: "{point_comparator} >= {interval_start} AND {point_comparator} <= {interval_end}",
    ast.TemporalComparisonOp.TEQUALS: "{point_comparator} = {interval_start} AND {point_comparator} = {interval_end}",
}

_param_placeholder: Final[str] = "?"


@dataclass
class _GeometrySql:
    sql_part: str


class DubkDBSQLEvaluator(Evaluator):
    def __init__(
        self,
        attribute_configs: List[AttributeConfig],
        function_map: Dict[str, str],
    ):
        self.geometry_attributes = {
            attribute.name: attribute.items_column
            for attribute in attribute_configs
            if attribute.is_geometry is True
        }
        self.temporal_attributes = {
            attribute.name: attribute.items_column
            for attribute in attribute_configs
            if attribute.is_temporal is True
        }
        self.attribute_column_map = {
            attribute.name: attribute.items_column for attribute in attribute_configs
        }
        self.attribute_type_map = {
            attribute.name: attribute.items_column_type
            for attribute in attribute_configs
        }
        self.function_map = function_map

    @handle(ast.Not)
    def not_(self, _, sub: FilterClause) -> FilterClause:
        return FilterClause(
            sql=f"NOT {sub.sql}",
            params=sub.params,
        )

    @handle(ast.And, ast.Or)
    def combination(
        self, node: ast.Combination, lhs: FilterClause, rhs: FilterClause
    ) -> FilterClause:
        return FilterClause(
            sql=f"({lhs.sql} {node.op.value} {rhs.sql})",
            params=lhs.params + rhs.params,
        )

    @handle(ast.Comparison, subclasses=True)
    def comparison(self, node: ast.Comparison, lhs, rhs) -> FilterClause:
        lhs_identifier, lhs_params, lhs_type = self._parameterise_node_part(
            node.lhs, lhs
        )
        rhs_identifier, rhs_params, rhs_type = self._parameterise_node_part(
            node.rhs, rhs
        )
        comparison_type = lhs_type or rhs_type
        lhs_part = (
            f"CAST({lhs_identifier} AS {comparison_type})"
            if comparison_type is not None and type(node.lhs) != ast.Attribute
            else lhs_identifier
        )
        rhs_part = (
            f"CAST({rhs_identifier} AS {comparison_type})"
            if comparison_type is not None and type(node.rhs) != ast.Attribute
            else rhs_identifier
        )
        return FilterClause(
            sql=f"({lhs_part} {_COMPARISON_OP_MAP[node.op]} {rhs_part})",
            params=lhs_params + rhs_params,
        )

    @handle(ast.Between)
    def between(self, node: ast.Between, lhs, low, high) -> FilterClause:
        lhs_identifier, lhs_params, lhs_type = self._parameterise_node_part(
            node.lhs, lhs
        )
        return FilterClause(
            sql=f"({lhs_identifier} {'NOT ' if node.not_ else ''}BETWEEN CAST(? AS {lhs_type}) AND CAST(? AS {lhs_type}))"
            if lhs_type is not None
            else f"({lhs_identifier} {'NOT ' if node.not_ else ''}BETWEEN ? AND ?)",
            params=lhs_params + [low, high],
        )

    @handle(ast.Like)
    def like(self, node: ast.Like, lhs: str) -> FilterClause:
        pattern = node.pattern
        if node.wildcard != "%":
            # TODO: not preceded by escapechar
            pattern = pattern.replace(node.wildcard, "%")
        if node.singlechar != "_":
            # TODO: not preceded by escapechar
            pattern = pattern.replace(node.singlechar, "_")
        lhs_identifier, lhs_params, _ = self._parameterise_node_part(node.lhs, lhs)
        return FilterClause(
            # TODO: handle node.nocase
            sql=f"{lhs_identifier} {'NOT ' if node.not_ else ''}LIKE "
            f"? ESCAPE '{node.escapechar}'",
            params=lhs_params + [pattern],
        )

    @handle(ast.In)
    def in_(self, node: ast.In, lhs, *options: Tuple[Any]) -> FilterClause:
        lhs_identifier, lhs_params, lhs_type = self._parameterise_node_part(
            node.lhs, lhs
        )
        return FilterClause(
            sql="{lhs_identifier} {not_logic}IN ({options})".format(
                lhs_identifier=lhs_identifier,
                not_logic="NOT " if node.not_ else "",
                options=", ".join(
                    [
                        f"CAST(? AS {lhs_type})" if lhs_type is not None else "?"
                        for _ in options
                    ]
                ),
            ),
            params=lhs_params + list(options),
        )

    @handle(ast.IsNull)
    def null(self, node: ast.IsNull, lhs) -> FilterClause:
        lhs_identifier, _, _ = self._parameterise_node_part(node.lhs, lhs)
        return FilterClause(
            sql=f"{lhs_identifier} IS {'NOT ' if node.not_ else ''}NULL"
        )

    @handle(*_TEMPORAL_POINT_COMPARISON_TYPES)
    def temporal_overlaps(self, node: ast.TemporalPredicate, lhs, rhs):
        lhs_identifier, lhs_params, _ = self._parameterise_node_part(node.lhs, lhs)
        rhs_identifier, rhs_params, _ = self._parameterise_node_part(node.rhs, rhs)
        if (
            isinstance(node.lhs, ast.Attribute)
            and node.lhs.name not in self.temporal_attributes
        ):
            raise NotATemporalField(node.lhs.name)
        if (
            isinstance(node.rhs, ast.Attribute)
            and node.rhs.name not in self.temporal_attributes
        ):
            raise NotATemporalField(node.rhs.name)
        if isinstance(node.lhs, ast.Attribute):
            point_comparator = lhs_identifier
        else:
            interval_start = interval_end = lhs_identifier
            if isinstance(node.lhs, values.Interval):
                lhs_params = [lhs_params[0].start, lhs_params[0].end]
        if isinstance(node.rhs, ast.Attribute):
            point_comparator = rhs_identifier
        else:
            interval_start = interval_end = rhs_identifier
            if isinstance(node.rhs, values.Interval):
                rhs_params = [rhs_params[0].start, rhs_params[0].end]
        return FilterClause(
            sql=_TEMPORAL_POINT_COMPARISON_OP_MAP[node.op].format(
                point_comparator=point_comparator,
                interval_start=interval_start,
                interval_end=interval_end,
            ),
            params=lhs_params + rhs_params,
        )

    @handle(values.Interval, subclasses=True)
    def interval(self, node: values.Interval, lhs, rhs) -> FilterClause:
        return node

    @handle(ast.SpatialComparisonPredicate, subclasses=True)
    def spatial_operation(
        self, node: ast.SpatialComparisonPredicate, lhs, rhs
    ) -> FilterClause:
        func = _SPATIAL_COMPARISON_OP_MAP[node.op]
        if type(lhs) is _GeometrySql and type(rhs) == _GeometrySql:
            return FilterClause(sql=f"{func}({lhs.sql_part},{rhs.sql_part})")
        if type(lhs) is not _GeometrySql:
            raise NotAGeometryField(lhs)
        if type(rhs) is not _GeometrySql:
            raise NotAGeometryField(rhs)
        raise Exception(f"unknown error condition in '{func}'")

    @handle(ast.BBox)
    def bbox(self, node: ast.BBox, lhs) -> FilterClause:
        if type(lhs) is not _GeometrySql:
            raise NotAGeometryField(lhs)
        return get_intersects_clause_for_bbox(
            node.minx,
            node.miny,
            node.maxx,
            node.maxy,
        )

    # inspired by https://github.com/geopython/pygeofilter/issues/90#issuecomment-2011712041
    @handle(ast.Attribute)
    def attribute(self, node: ast.Attribute) -> str | _GeometrySql:
        if node.name in self.geometry_attributes:
            return _GeometrySql(sql_part=f"{node.name}")
        try:
            return f'"{self.attribute_column_map[node.name]}"'
        except KeyError as e:
            raise UnknownField(field_name=str(e))

    @handle(ast.Arithmetic, subclasses=True)
    def arithmetic(self, node: ast.Arithmetic, lhs, rhs):
        op = _ARITHMETIC_OP_MAP[node.op]
        lhs_identifier, lhs_params, lhs_type = self._parameterise_node_part(
            node.lhs, lhs
        )
        rhs_identifier, rhs_params, rhs_type = self._parameterise_node_part(
            node.rhs, rhs
        )
        lhs_part = (
            f"CAST({lhs_identifier} AS {lhs_type})"
            if lhs_type is not None
            else lhs_identifier
        )
        rhs_part = (
            f"CAST({rhs_identifier} AS {rhs_type})"
            if rhs_type is not None
            else rhs_identifier
        )
        return FilterClause(
            sql=f"({lhs_part} {op} {rhs_part})",
            params=lhs_params + rhs_params,
        )

    @handle(ast.Function)
    def function(self, node, *arguments):
        try:
            func = self.function_map[node.name]
        except KeyError as e:
            raise UnknownFunction(field_name=str(e))
        return f"{func}({','.join(arguments)})"

    @handle(*values.LITERALS)
    def literal(self, node):
        return str(node)

    # inspired by https://github.com/geopython/pygeofilter/issues/90#issuecomment-2011712041
    @handle(values.Geometry)
    def geometry(self, node: values.Geometry) -> _GeometrySql:
        wkb_hex = geometry.shape(node).wkb_hex
        return _GeometrySql(sql_part=f"ST_GeomFromHEXEWKB('{wkb_hex}')")

    # inspired by https://github.com/geopython/pygeofilter/issues/90#issuecomment-2011712041
    @handle(values.Envelope)
    def envelope(self, node: values.Envelope) -> _GeometrySql:
        wkb_hex = geometry.box(node.x1, node.y1, node.x2, node.y2).wkb_hex
        return _GeometrySql(sql_part=f"ST_GeomFromHEXEWKB('{wkb_hex}')")

    def adopt_result(self, result: FilterClause) -> FilterClause:
        # flatten any nested FilterClauses as DuckDB will not evaluate parameterised expressions that are themselves parameters
        def elevate_nested_clauses(parent_clause: FilterClause) -> FilterClause:
            for i, param in enumerate(parent_clause.params):
                if type(param) is FilterClause:
                    param = elevate_nested_clauses(param)
                    parts = parent_clause.sql.split(_param_placeholder)
                    parent_clause.sql = (
                        _param_placeholder.join(parts[: i + 1])
                        + param.sql
                        + _param_placeholder.join(parts[i + 1 :])
                    )
                    parent_clause.params = (
                        parent_clause.params[:i]
                        + param.params
                        + parent_clause.params[i + 1 :]
                    )
            return parent_clause

        return elevate_nested_clauses(result)

    def _parameterise_node_part(
        self, node_part: ast.Node, part_value: Any
    ) -> Tuple[Any, List[Any], Optional[str]]:
        part_params = []
        part_identifier = _param_placeholder
        part_type = None
        if type(node_part) == ast.Attribute:
            # not an issue here that the field name remains quoted
            part_identifier = part_value
            part_type = self.attribute_type_map[node_part.name]
        else:
            part_params.append(part_value)
        return (part_identifier, part_params, part_type)


def to_filter_clause(
    root: ast.Node,
    attribute_configs: List[AttributeConfig],
) -> FilterClause:
    return DubkDBSQLEvaluator(
        attribute_configs,
        cast(Dict[str, str], {}),
    ).evaluate(root)
