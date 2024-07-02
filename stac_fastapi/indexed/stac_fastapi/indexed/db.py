import logging
import os
from logging import Logger, getLogger
from typing import Final, List, Type, cast
from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from fastapi import FastAPI

from stac_fastapi_indexed.index_source.file import FileIndexSource
from stac_fastapi_indexed.index_source.index_source import IndexSource
from stac_fastapi_indexed.index_source.s3 import S3IndexSource
from stac_fastapi_indexed.settings import get_settings
from stac_fastapi_indexed.util import utc_now

_logger: Final[Logger] = getLogger(__file__)
_index_sources: Final[List[Type[IndexSource]]] = [
    S3IndexSource,
    FileIndexSource,
]


def connect_to_db(app: FastAPI) -> None:
    index_source_url = get_settings().parquet_index_source_url
    compatible_index_sources = [
        index_source
        for index_source in [
            candidate.create_index_source(index_source_url)
            for candidate in _index_sources
        ]
        if index_source is not None
    ]
    if len(compatible_index_sources) == 0:
        raise Exception(f"no index sources support source URL '{index_source_url}'")
    index_source = cast(IndexSource, compatible_index_sources[0])
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
    parquet_urls = index_source.get_parquet_urls()
    if len(parquet_urls.keys()) == 0:
        raise Exception(f"no URLs found at '{index_source_url}'")
    for view_name, source_url in parquet_urls.items():
        _logger.debug(f"Source URL:\n{source_url}")
        command = f"CREATE VIEW {view_name} AS SELECT * FROM '{source_url}'"
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


def disconnect_from_db(app: FastAPI) -> None:
    if hasattr(app.state, "db_connection") and app.state.db_connection is not None:
        try:
            cast(DuckDBPyConnection, app.state.db_connection).close()
        except Exception as e:
            _logger.error(e)
