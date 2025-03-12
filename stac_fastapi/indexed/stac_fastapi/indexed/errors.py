from typing import List

from stac_fastapi.indexed.db import fetchall
from stac_index.common.indexing_error import IndexingError


def get_all_errors() -> List[IndexingError]:
    """Query the database for the list of errors that occured during indexing."""
    return [
        IndexingError(
            timestamp=row[0],
            type=row[1],
            subtype=row[2],
            input_location=row[3],
            description=row[4],
            possible_fixes=row[5],
        )
        for row in fetchall("""
        SELECT time
             , error_type
             , subtype
             , input_location
             , description
             , possible_fixes
         FROM errors
     ORDER BY id
        """)
    ]
