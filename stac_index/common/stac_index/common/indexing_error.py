from datetime import datetime, timezone
from enum import Enum

from duckdb import DuckDBPyConnection
from pydantic import BaseModel


class IndexingErrorType(str, Enum):
    unknown = "unknown"
    item_fetching = "item_fetching"
    item_parsing = "item_parsing"
    item_validation = "item_parsing"
    collection_parsing = "collection_parsing"


class IndexingError(BaseModel):
    timestamp: datetime
    type_: IndexingErrorType
    description: str


def new_error(type_: IndexingErrorType, description: str) -> IndexingError:
    return IndexingError(
        timestamp=datetime.now(tz=timezone.utc), type_=type_, description=description
    )


def get_all_errors(db_conn: DuckDBPyConnection) -> list[IndexingError]:
    query = db_conn.sql("SELECT time, error_type, description FROM errors ORDER BY id;")
    return [
        IndexingError(timestamp=row[0], type_=row[1], description=row[2])
        for row in query.fetchall()
    ]


def save_error(db_conn: DuckDBPyConnection, error: IndexingError):
    db_conn.execute(
        "INSERT INTO errors (time, error_type, description) VALUES (?, ?, ?)",
        (error.timestamp, error.type_, error.description),
    )
