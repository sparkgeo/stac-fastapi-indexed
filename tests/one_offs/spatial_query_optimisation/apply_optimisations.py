from os import environ
from typing import Dict, Final, List, Optional, Tuple

import mercantile
from duckdb import connect as duckdb_connect
from shapely.geometry import Polygon
from shapely.wkt import loads as loads_wkt

_test_limit_arg = environ.get("TEST_ROW_LIMIT", None)
_test_limit = None if _test_limit_arg is None else int(_test_limit_arg)

_minimum_bounding_quadkey_column_name: Final[str] = "minimum_bounding_quadkey"
_composite_key_column_name: Final[str] = "unique_id"
_bbox_parts_column_names: Final[Dict[str, str]] = {
    "minx": "bbox_x_min",
    "miny": "bbox_y_min",
    "maxx": "bbox_x_max",
    "maxy": "bbox_y_max",
}


def execute(
    source_path: str,
) -> None:
    connection = duckdb_connect()
    connection.execute("INSTALL spatial; LOAD spatial")
    connection.execute(
        "CREATE TABLE items AS SELECT * FROM '{}'".format(
            source_path,
        )
    )
    existing_columns = [
        row[0] for row in connection.execute("DESCRIBE items").fetchall()
    ]
    updates: Dict[str, List[Tuple[str, str, str]]] = {}
    update_args: List[Tuple[str, str, str]]
    polygon: Polygon
    if _minimum_bounding_quadkey_column_name in existing_columns:
        print(
            "'{}' already exists, skipping".format(
                _minimum_bounding_quadkey_column_name
            )
        )
    else:
        update_args = []
        connection.execute(
            "ALTER TABLE items ADD {} VARCHAR".format(
                _minimum_bounding_quadkey_column_name
            )
        )
        connection.execute(
            "ALTER TABLE items ADD {} VARCHAR".format(_composite_key_column_name)
        )
        for row in connection.execute(
            "SELECT collection_id, id, ST_AsText(ST_GeomFromWKB(geometry)) FROM items{}".format(
                f" LIMIT {_test_limit}" if _test_limit is not None else ""
            )
        ).fetchall():
            collection_id, id, wkt = row
            composite_key = f"{collection_id}_{id}"
            row_update_args: List[Optional[str]] = [composite_key]
            polygon = loads_wkt(wkt)
            if polygon.is_empty:
                row_update_args.append(None)
            else:
                row_update_args.append(
                    mercantile.quadkey(mercantile.bounding_tile(*polygon.bounds))
                )
            update_args.append(
                (
                    *row_update_args,
                    collection_id,
                    id,
                )
            )
        updates[
            "UPDATE items SET {} = ?, {} = ? WHERE collection_id = ? and id = ?".format(
                _composite_key_column_name,
                _minimum_bounding_quadkey_column_name,
            )
        ] = update_args

    if _bbox_parts_column_names["minx"] in existing_columns:
        print("'{}' already exists, skipping".format(_bbox_parts_column_names["minx"]))
    else:
        update_args = []
        for column_name in _bbox_parts_column_names.values():
            connection.execute("ALTER TABLE items ADD {} REAL".format(column_name))
        for row in connection.execute(
            "SELECT collection_id, id, ST_AsText(ST_GeomFromWKB(geometry)) FROM items{}".format(
                f" LIMIT {_test_limit}" if _test_limit is not None else ""
            )
        ).fetchall():
            collection_id, id, wkt = row
            polygon = loads_wkt(wkt)
            if polygon.is_empty:
                continue
            update_args.append(
                (
                    *polygon.bounds,
                    collection_id,
                    id,
                )
            )
        updates[
            "UPDATE items SET {} = ?, {} = ?, {} = ?, {} = ? WHERE collection_id = ? and id = ?".format(
                *_bbox_parts_column_names.values()
            )
        ] = update_args

    for statement, args in updates.items():
        connection.executemany(
            statement,
            args,
        )
    output_path = ".".join(
        source_path.split(".")[:-1] + ["optimised"] + source_path.split(".")[-1:]
    )
    connection.execute(
        """
    COPY (SELECT * FROM items) TO '{}' (FORMAT PARQUET)
    """.format(output_path)
    )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "source_path",
        type=str,
        help="File path for the items.parquet file that needs updating",
    )
    args = parser.parse_args()
    execute(
        source_path=args.source_path,
    )
