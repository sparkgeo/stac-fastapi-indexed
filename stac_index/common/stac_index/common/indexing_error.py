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
    type: IndexingErrorType
    subtype: str
    input_location: str
    description: str


def new_error(
    type: IndexingErrorType,
    description: str,
    *,
    subtype: str = "",
    input_location: str = "",
) -> IndexingError:
    return IndexingError(
        timestamp=datetime.now(tz=timezone.utc),
        type=type,
        subtype=subtype,
        input_location=input_location,
        description=description,
    )


def get_all_errors(db_conn: DuckDBPyConnection) -> list[IndexingError]:
    query = db_conn.sql(
        "SELECT time, error_type, subtype, input_location, description FROM errors ORDER BY id;"
    )
    return [
        IndexingError(
            timestamp=row[0],
            type=row[1],
            subtype=row[2],
            input_location=row[3],
            description=row[4],
        )
        for row in query.fetchall()
    ]


def save_error(db_conn: DuckDBPyConnection, error: IndexingError):
    db_conn.execute(
        "INSERT INTO errors (time, error_type, subtype, input_location, description) VALUES (?, ?, ?, ?, ?)",
        (
            error.timestamp,
            error.type,
            error.subtype,
            error.input_location,
            error.description,
        ),
    )
