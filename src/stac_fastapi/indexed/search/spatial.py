from stac_fastapi.indexed.search.filter_clause import FilterClause


def get_intersects_clause_for_wkt(wkt: str) -> FilterClause:
    return FilterClause(
        sql=f"""
            ST_Intersects(
                geometry,
                ST_GeomFromText('{wkt}')
            )
        """
    )


def get_intersects_clause_for_bbox(
    x_min: float, y_min: float, x_max: float, y_max: float
) -> FilterClause:
    return get_intersects_clause_for_wkt(
        f"POLYGON (({x_min} {y_min}, {x_max} {y_min}, {x_max} {y_max}, {x_min} {y_max}, {x_min} {y_min}))"
    )
