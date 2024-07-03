from functools import lru_cache
from logging import Logger, getLogger
from time import time
from typing import Any, Final, List, Optional

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect

from stac_fastapi.indexed.settings import get_settings
from stac_index.common import index_reader_classes

_logger: Final[Logger] = getLogger(__file__)
_query_timing_precision: Final[int] = 3
_root_db_connection: DuckDBPyConnection = None


async def connect_to_db() -> None:
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
    start = time()
    global _root_db_connection
    _root_db_connection = duckdb_connect()
    times["create db connection"] = time()
    for config_command in index_source.get_duckdb_configuration_statements():
        execute(
            config_command[0],
            config_command[1] if len(config_command) > 1 else None,
        )
    times["index source configuration"] = time()
    execute("INSTALL spatial")
    execute("LOAD spatial")
    times["load spatial extension"] = time()
    execute("INSTALL httpfs")
    execute("LOAD httpfs")
    times["load httpfs extension"] = time()
    parquet_uris = await index_source.get_parquet_uris()
    if len(parquet_uris.keys()) == 0:
        raise Exception(f"no URIs found at '{index_source_uri}'")
    for view_name, source_uri in parquet_uris.items():
        execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{source_uri}'")
    times["create views from parquet"] = time()
    for operation, completed_at in times.items():
        _logger.info(
            "'{}' completed in {}s".format(
                operation, round(completed_at - start, _query_timing_precision)
            )
        )
        start = completed_at


async def disconnect_from_db() -> None:
    if _root_db_connection is not None:
        try:
            _root_db_connection.close()
        except Exception as e:
            _logger.error(e)


@lru_cache(maxsize=1)
def get_db_connection():
    return _root_db_connection


def execute(statement: str, params: Optional[List[Any]] = None) -> None:
    start = time()
    get_db_connection().execute(statement, params)
    _sql_log_message(statement, time() - start, None, params)


def fetchone(statement: str, params: Optional[List[Any]] = None) -> Any:
    start = time()
    result = get_db_connection().execute(statement, params).fetchone()
    _sql_log_message(statement, time() - start, 1 if result is not None else 0, params)
    return result


def fetchall(statement: str, params: Optional[List[Any]] = None) -> List[Any]:
    start = time()
    result = get_db_connection().execute(statement, params).fetchall()
    _sql_log_message(statement, time() - start, len(result), params)
    return result


def _sql_log_message(
    statement: str,
    duration: float,
    result_size: Optional[int] = None,
    params: Optional[List[Any]] = None,
) -> None:
    # None of the queries logged by this API are expected to contain sensitive data
    _logger.debug(
        "SQL: {statement}; Params: {params}; in {time_info}s{result_info}".format(
            statement=statement,
            params=params,
            time_info=round(duration, _query_timing_precision),
            result_info="" if result_size is None else f" {result_size} row(s)",
        )
    )
