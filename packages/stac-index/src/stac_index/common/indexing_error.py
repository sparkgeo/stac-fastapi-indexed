from datetime import datetime, timezone
from enum import Enum
from typing import Optional

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
    description: str
    collection: Optional[str] = None
    item: Optional[str] = None
    subtype: Optional[str] = None
    input_location: Optional[str] = None
    possible_fixes: Optional[str] = None


def new_error(
    type: IndexingErrorType,
    description: str,
    *,
    subtype: Optional[str] = None,
    input_location: Optional[str] = None,
    possible_fixes: Optional[str] = None,
    collection: Optional[str] = None,
    item: Optional[str] = None,
) -> IndexingError:
    """Convenience method for constructing an IndexingError."""
    return IndexingError(
        timestamp=datetime.now(tz=timezone.utc),
        type=type,
        subtype=subtype,
        input_location=input_location,
        description=description,
        possible_fixes=possible_fixes,
        collection=collection,
        item=item,
    )


def save_error(db_conn: DuckDBPyConnection, error: IndexingError):
    """Write an error to the database."""
    db_conn.execute(
        "INSERT INTO errors (time, error_type, subtype, input_location, description, possible_fixes, collection, item) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            error.timestamp,
            error.type,
            error.subtype,
            error.input_location,
            error.description,
            error.possible_fixes,
            error.collection,
            error.item,
        ),
    )
