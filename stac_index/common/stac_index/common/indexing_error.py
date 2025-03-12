from datetime import datetime, timezone
from enum import Enum

from duckdb import DuckDBPyConnection
from pydantic import BaseModel


class IndexingErrorType(str, Enum):
    unknown = "unknown"
    item_fetching = "item_fetching"
    item_parsing = "item_parsing"
    item_validation = "item_validation"
    collection_parsing = "collection_parsing"


class IndexingError(BaseModel):
    """Represents an error that occured during indexing.

    This error type is stored in the index and returned by the API.

    """

    timestamp: datetime
    type: IndexingErrorType
    subtype: str
    input_location: str
    description: str
    possible_fixes: str


def new_error(
    type: IndexingErrorType,
    description: str,
    *,
    subtype: str = "",
    input_location: str = "",
    possible_fixes: str = "",
) -> IndexingError:
    """Convenience method for constructing an IndexingError."""
    return IndexingError(
        timestamp=datetime.now(tz=timezone.utc),
        type=type,
        subtype=subtype,
        input_location=input_location,
        description=description,
        possible_fixes=possible_fixes,
    )


def save_error(db_conn: DuckDBPyConnection, error: IndexingError):
    """Write an error to the database."""
    db_conn.execute(
        "INSERT INTO errors (time, error_type, subtype, input_location, description, possible_fixes) VALUES (?, ?, ?, ?, ?, ?)",
        (
            error.timestamp,
            error.type,
            error.subtype,
            error.input_location,
            error.description,
            error.possible_fixes,
        ),
    )
