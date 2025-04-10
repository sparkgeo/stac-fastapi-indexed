import re
from logging import Logger, getLogger
from os import environ
from time import time
from typing import Any, Dict, Final, List, Optional

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from stac_index.common import index_reader_classes

from stac_fastapi.indexed.settings import get_settings

_logger: Final[Logger] = getLogger(__name__)
_query_timing_precision: Final[int] = 3
_query_object_identifier_prefix: Final[str] = "src:"
_query_object_identifier_suffix: Final[str] = ":src"
_query_object_identifier_template: Final[str] = (
    f"{_query_object_identifier_prefix}{{}}{_query_object_identifier_suffix}"
)

_root_db_connection: DuckDBPyConnection = None
_parquet_uris: Dict[str, str] = {}


async def connect_to_db() -> None:
    settings = get_settings()
    index_manifest_uri = settings.index_manifest_uri
    compatible_index_readers = [
        index_reader
        for index_reader in index_reader_classes
        if index_reader.can_handle_source_uri(index_manifest_uri)
    ]
    if len(compatible_index_readers) == 0:
        raise Exception(f"no index readers support manifest URI '{index_manifest_uri}'")
    index_source = compatible_index_readers[0](index_manifest_uri)
    times = {}
    start = time()
    global _root_db_connection
    _root_db_connection = duckdb_connect()
    times["create db connection"] = time()
    for config_command in index_source.get_duckdb_configuration_statements():
        execute(
            config_command[0],
            config_command[1],
        )
    times["index source configuration"] = time()
    if settings.install_duckdb_extensions:
        # Dockerfiles pre-install extensions, so don't need installing here.
        # Local debug (e.g. running in vscode) still requires this install.
        execute("INSTALL spatial")
        times["install spatial extension"] = time()
        execute("INSTALL httpfs")
        times["install httpfs extension"] = time()
    execute("LOAD spatial")
    times["load spatial extension"] = time()
    execute("LOAD httpfs")
    times["load httpfs extension"] = time()
    duckdb_thread_count = settings.duckdb_threads
    if duckdb_thread_count:
        _set_duckdb_threads(duckdb_thread_count)
    global _parquet_uris
    _parquet_uris = await index_source.get_parquet_uris()
    times["get parquet URIs"] = time()
    if len(_parquet_uris.keys()) == 0:
        raise Exception(f"no URIs found from '{index_manifest_uri}'")
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


# SQL queries include placeholder strings that are replaced with Parquet URIs prior to query execution.
# This improves query performance relative to creating views in DuckDB from Parquet files and querying those.
# Placeholders are used until the point of query execution so that API search pagination tokens,
# which are JWT-encoded SQL queries and visible to the client, do not leak implementation detail around
# parquet URI locations.
def format_query_object_name(object_name: str) -> str:
    return _query_object_identifier_template.format(object_name)


def execute(statement: str, params: Optional[List[Any]] = None) -> None:
    start = time()
    statement = _prepare_statement(statement)
    _get_db_connection().execute(statement, params)
    _sql_log_message(statement, time() - start, None, params)


def fetchone(statement: str, params: Optional[List[Any]] = None) -> Any:
    start = time()
    statement = _prepare_statement(statement)
    result = _get_db_connection().execute(statement, params).fetchone()
    _sql_log_message(statement, time() - start, 1 if result is not None else 0, params)
    return result


def fetchall(statement: str, params: Optional[List[Any]] = None) -> List[Any]:
    start = time()
    statement = _prepare_statement(statement)
    result = _get_db_connection().execute(statement, params).fetchall()
    _sql_log_message(statement, time() - start, len(result), params)
    return result


def _get_db_connection():
    return _root_db_connection.cursor()


def _prepare_statement(statement: str) -> str:
    query_object_identifier_regex = rf"\b{re.escape(_query_object_identifier_prefix)}([^:]+){re.escape(_query_object_identifier_suffix)}\b"
    for query_object_name in re.findall(query_object_identifier_regex, statement):
        if query_object_name not in _parquet_uris:
            _logger.warning(
                f"{query_object_name} not in parquet URI map, query will likely fail"
            )
            continue
        statement = re.sub(
            rf"\b{re.escape(_query_object_identifier_prefix)}{re.escape(query_object_name)}{re.escape(_query_object_identifier_suffix)}\b",
            f"'{_parquet_uris[query_object_name]}'",
            statement,
        )
    return statement


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


def _set_duckdb_threads(duckdb_thread_count: int) -> None:
    duckdb_required_memory_mb = (
        duckdb_thread_count * 125
    )  # duckdb suggest 125mb per thread
    lambda_memory_mb = environ.get(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", None
    )  # this is a reserved AWS env var, not defined by this application
    if lambda_memory_mb:
        if int(lambda_memory_mb) < duckdb_required_memory_mb:
            raise MemoryError(
                "duckdb {} threads requires: '{}MB'. Lambda memory: '{}MB'".format(
                    duckdb_thread_count,
                    duckdb_required_memory_mb,
                    lambda_memory_mb,
                )
            )
    execute(f"SET threads to {duckdb_thread_count}")
