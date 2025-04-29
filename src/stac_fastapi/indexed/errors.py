from typing import List

from stac_index.indexer.types.indexing_error import IndexingError

from stac_fastapi.indexed.db import fetchall, format_query_object_name


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
            collection=row[6],
            item=row[7],
        )
        for row in fetchall(f"""
        SELECT time
             , error_type
             , subtype
             , input_location
             , description
             , possible_fixes
             , collection
             , item
         FROM {format_query_object_name('errors')}
     ORDER BY id
        """)
    ]
