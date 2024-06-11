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
    times = {}
    start = utc_now()
    duckdb_connection = duckdb_connect()
    times["create db connection"] = utc_now()
    duckdb_connection.execute("INSTALL spatial")
    duckdb_connection.execute("LOAD spatial")
    times["load spatial extension"] = utc_now()
    duckdb_connection.execute("INSTALL httpfs")
    duckdb_connection.execute("LOAD httpfs")
    times["load httpfs extension"] = utc_now()
    for parquet_path in glob(
        path.join(get_settings().parquet_source_data_dir, "*.parquet")
    ):
        view_name = path.basename(".".join(parquet_path.split(".")[:-1]))
        duckdb_connection.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_path}'"
        )
    times["create views from parquet"] = utc_now()
    for operation, completed_at in times.items():
        _logger.debug(
            "'{}' completed in {}ms".format(
                operation, (completed_at - start).microseconds / 1000
            )
        )
        start = completed_at
    app.state.db_connection = duckdb_connection


def disconnect_from_db(app: FastAPI) -> None:
    if hasattr(app.state, "db_connection") and app.state.db_connection is not None:
        try:
            cast(DuckDBPyConnection, app.state.db_connection).close()
        except Exception as e:
            _logger.error(e)
