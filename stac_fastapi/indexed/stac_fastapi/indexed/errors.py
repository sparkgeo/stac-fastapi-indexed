import stac_index.common.indexing_error as errors
from stac_fastapi.indexed.db import get_db_connection


def get_all_errors() -> list[errors.IndexingError]:
    return errors.get_all_errors(get_db_connection())
