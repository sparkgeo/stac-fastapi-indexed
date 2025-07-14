from datetime import UTC, datetime
from logging import Logger, getLogger
from os import environ
from time import time
from typing import Any, Dict, Final, List, Optional

from duckdb import DuckDBPyConnection
from duckdb import connect as duckdb_connect
from stac_index.indexer.creator.creator import IndexCreator
from stac_index.io.readers import get_reader_for_uri
from stac_index.io.readers.exceptions import MissingIndexException
from stac_index.io.readers.source_reader import IndexReader

from stac_fastapi.indexed.settings import get_settings

_logger: Final[Logger] = getLogger(__name__)
_query_timing_precision: Final[int] = 3

_root_db_connection: DuckDBPyConnection = None
_parquet_uris: Dict[str, str] = {}
_index_manifest_last_modified: int = 0
_last_load_id: Optional[str] = None


async def connect_to_db() -> None:
    times: Dict[str, float] = {}
    settings = get_settings()
    index_manifest_uri = settings.index_manifest_uri
    source_reader = get_reader_for_uri(uri=index_manifest_uri)
    index_reader = source_reader.get_index_reader(index_manifest_uri=index_manifest_uri)
    start = time()
    await _ensure_latest_data()
    times["set data versioning variables"] = time()
    global _root_db_connection
    _root_db_connection = duckdb_connect()
    times["create db connection"] = time()
    for config_command in index_reader.get_duckdb_configuration_statements():
        _execute(
            config_command[0],
            config_command[1],
        )
    times["index source configuration"] = time()
    if settings.install_duckdb_extensions:
        # Dockerfiles pre-install extensions, so don't need installing here.
        # Local debug (e.g. running in vscode) still requires this install.
        _execute("INSTALL spatial")
        times["install spatial extension"] = time()
        _execute("INSTALL httpfs")
        times["install httpfs extension"] = time()
    _execute("LOAD spatial")
    times["load spatial extension"] = time()
    _execute("LOAD httpfs")
    times["load httpfs extension"] = time()
    if settings.duckdb_threads:
        _set_duckdb_threads(settings.duckdb_threads)
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


def format_query_object_name(object_name: str) -> str:
    if object_name in _parquet_uris:
        return "'{}'".format(_parquet_uris[object_name])
    raise Exception(
        "Attempt to use non-existent query object name '{bad_name}'. Available object names: '{availables}'".format(
            bad_name=object_name,
            availables="', '".join(list(_parquet_uris.keys())),
        )
    )


def _execute(statement: str, params: Optional[List[Any]] = None) -> None:
    start = time()
    _get_db_connection().execute(statement, params)
    _sql_log_message(statement, time() - start, None, params)


async def fetchone(
    statement: str,
    params: Optional[List[Any]] = None,
    perform_latest_data_check: bool = True,
) -> Any:
    if perform_latest_data_check:
        await _ensure_latest_data()
    start = time()
    result = _get_db_connection().execute(statement, params).fetchone()
    _sql_log_message(statement, time() - start, 1 if result is not None else 0, params)
    return result


async def fetchall(
    statement: str,
    params: Optional[List[Any]] = None,
    perform_latest_data_check: bool = True,
) -> List[Any]:
    if perform_latest_data_check:
        await _ensure_latest_data()
    start = time()
    result = _get_db_connection().execute(statement, params).fetchall()
    _sql_log_message(statement, time() - start, len(result), params)
    return result


def get_last_load_id() -> str:
    if _last_load_id is None:
        raise Exception("attempt to access load id before set")
    return _last_load_id


def _get_db_connection():
    return _root_db_connection.cursor()


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
    _execute(f"SET threads to {duckdb_thread_count}")


async def _ensure_latest_data() -> None:
    global _index_manifest_last_modified
    start = time()
    index_manifest_uri = get_settings().index_manifest_uri
    source_reader = get_reader_for_uri(uri=index_manifest_uri)
    new_last_modified = await source_reader.get_last_modified_epoch_for_uri(
        uri=index_manifest_uri
    ) or round(datetime.now(tz=UTC).timestamp())
    if new_last_modified != _index_manifest_last_modified:
        _logger.warning("index manifest has changed, reloading data")
        await _set_parquet_uris(
            source_reader.get_index_reader(index_manifest_uri=index_manifest_uri)
        )
        await _set_last_load_id()
        _index_manifest_last_modified = new_last_modified
    _logger.debug(
        f"ensured latest data in {round(time() - start, _query_timing_precision)}s"
    )


async def _set_last_load_id() -> None:
    index_manifest_uri = get_settings().index_manifest_uri
    source_reader = get_reader_for_uri(uri=index_manifest_uri)
    index_manifest = await source_reader.get_index_reader(
        index_manifest_uri=index_manifest_uri
    ).get_index_manifest()
    global _last_load_id
    _last_load_id = index_manifest.load_id


async def _set_parquet_uris(index_reader: IndexReader) -> None:
    global _parquet_uris
    try:
        _parquet_uris = await index_reader.get_parquet_uris()
    except MissingIndexException:
        _logger.warning("index missing")
        settings = get_settings()
        if settings.create_empty_index_if_missing:
            settings.index_manifest_uri = IndexCreator().create_empty()
            source_reader = get_reader_for_uri(uri=settings.index_manifest_uri)
            index_reader = source_reader.get_index_reader(
                index_manifest_uri=settings.index_manifest_uri
            )
            _parquet_uris = await index_reader.get_parquet_uris()
        else:
            raise Exception(
                "not configured to create empty index if missing, cannot proceed"
            )
