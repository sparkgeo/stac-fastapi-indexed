from logging import Logger, getLogger
from typing import Final, cast

import boto3
from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from fastapi import FastAPI

from stac_fastapi.indexed.settings import get_settings
from stac_fastapi.indexed.util import utc_now
from stac_index.common import index_reader_classes

_logger: Final[Logger] = getLogger(__file__)


async def connect_to_db(app: FastAPI) -> None:
    index_source_uri = get_settings().parquet_index_source_uri
    compatible_index_readers = [
        index_reader
        for index_reader in index_reader_classes
        if index_reader.can_handle_source_uri(index_source_uri)
    ]
    if len(compatible_index_readers) == 0:
        raise Exception(f"no index readers support source URI '{index_source_uri}'")
    index_source = compatible_index_readers[0](index_source_uri)
    times = {}
    start = utc_now()
    duckdb_connection = duckdb_connect()
    _logger.debug("Connect duckdb success")
    index_source.configure_duckdb(duckdb_connection)
    _logger.debug("Configure duckdb success")
    times["create db connection"] = utc_now()
    duckdb_connection.execute("INSTALL spatial")
    _logger.debug("Install Spatial Success")
    duckdb_connection.execute("LOAD spatial")
    _logger.debug("Load Spatial Success")
    times["load spatial extension"] = utc_now()
    duckdb_connection.execute("INSTALL httpfs")
    _logger.debug("Install httpfs Success")
    duckdb_connection.execute("LOAD httpfs")
    _logger.debug("Load httpfs Success")
    times["load httpfs extension"] = utc_now()
    parquet_urls = await index_source.get_parquet_uris()
    if len(parquet_urls.keys()) == 0:
        raise Exception(f"no URLs found at '{index_source_uri}'")
    for view_name, source_uri in parquet_urls.items():
        command = f"CREATE VIEW {view_name} AS SELECT * FROM '{source_uri}'"
        _logger.debug(command)
        duckdb_connection.execute(command)
    times["create views from parquet"] = utc_now()
    for operation, completed_at in times.items():
        _logger.debug(
            "'{}' completed in {}ms".format(
                operation, (completed_at - start).microseconds / 1000
            )
        )
        start = completed_at
    app.state.db_connection = duckdb_connection


async def disconnect_from_db(app: FastAPI) -> None:
    if hasattr(app.state, "db_connection") and app.state.db_connection is not None:
        try:
            cast(DuckDBPyConnection, app.state.db_connection).close()
        except Exception as e:
            _logger.error(e)
