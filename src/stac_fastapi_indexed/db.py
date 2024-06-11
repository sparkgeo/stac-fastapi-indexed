from glob import glob
from logging import Logger, getLogger
from os import path
from typing import Final, cast

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from fastapi import FastAPI

from stac_fastapi_indexed.settings import get_settings
from stac_fastapi_indexed.util import utc_now

_logger: Final[Logger] = getLogger(__file__)


def connect_to_db(app: FastAPI) -> None:
    start = utc_now()
    times = {}
    app.state.db_connection = duckdb_connect()
    times["create db connection"] = utc_now()
    app.state.db_connection.execute("INSTALL spatial")
    app.state.db_connection.execute("LOAD spatial")
    times["load spatial extension"] = utc_now()
    app.state.db_connection.execute("INSTALL httpfs")
    app.state.db_connection.execute("LOAD httpfs")
    times["load httpfs extension"] = utc_now()
    _establish_data_connection(app.state.db_connection)
    times["create views from parquet"] = utc_now()
    for row in app.state.db_connection.execute("SELECT COUNT(*) FROM items").fetchall():
        _logger.debug(row)
    times["count items"] = utc_now()

    reference_point = start
    for operation, completed_at in times.items():
        _logger.debug(
            "'{}' completed in {}ms".format(
                operation, (completed_at - reference_point).microseconds / 1000
            )
        )
        reference_point = completed_at


def disconnect_from_db(app: FastAPI) -> None:
    if hasattr(app.state, "db_connection") and app.state.db_connection is not None:
        try:
            cast(DuckDBPyConnection, app.state.db_connection).close()
        except Exception as e:
            _logger.error(e)


def _establish_data_connection(connection: DuckDBPyConnection) -> None:
    for parquet_path in glob(
        path.join(get_settings().parquet_source_data_dir, "*.parquet")
    ):
        view_name = path.basename(".".join(parquet_path.split(".")[:-1]))
        connection.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_path}'")
