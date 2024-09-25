from shapely.wkt import loads as loads_wkt

from stac_fastapi.indexed.search.filter_clause import FilterClause


def get_intersects_clause_for_wkt(wkt: str) -> FilterClause:
    geometry = loads_wkt(wkt)
    return _get_intersects_clause_for_wrapped_geometry(
        f"ST_GeomFromText('{wkt}')",
        *geometry.bounds,
    )


def get_intersects_clause_for_bbox(
    x_min: float, y_min: float, x_max: float, y_max: float
) -> FilterClause:
    return get_intersects_clause_for_wkt(
        f"POLYGON (({x_min} {y_min}, {x_max} {y_min}, {x_max} {y_max}, {x_min} {y_max}, {x_min} {y_min}))"
    )


def _get_intersects_clause_for_wrapped_geometry(
    sql_wrapped_geometry: str, x_min: float, y_min: float, x_max: float, y_max: float
) -> FilterClause:
    return FilterClause(
        sql=f"""
            NOT (
                    bbox_x_max < ?
                    OR bbox_y_max < ?
                    OR bbox_x_min > ?
                    OR bbox_y_min > ?
                )
            AND ST_Intersects(
                    geometry,
                    {sql_wrapped_geometry}
                )
        """,
        params=[
            x_min,
            y_min,
            x_max,
            y_max,
        ],
    )
