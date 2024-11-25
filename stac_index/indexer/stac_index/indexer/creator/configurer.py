from json import dumps
from typing import List, Tuple

from duckdb import DuckDBPyConnection

from stac_index.indexer.types.index_config import IndexConfig


def add_items_columns(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    for indexable in config.indexables.values():
        connection.execute(
            f"""
            ALTER TABLE items
            ADD COLUMN {indexable.table_column_name} {indexable.storage_type.value}
        """
        )


def configure_indexables(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    _configure_queryables(config, connection)
    _configure_sortables(config, connection)


def _configure_queryables(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    queryables_collections: List[Tuple[str, str]] = []
    for name, queryable in config.queryables.items():
        indexable = config.indexables[name]
        for collection_id in queryable.collections:
            queryables_collections.append((collection_id, name))
        connection.execute(
            """
            INSERT INTO queryables (name, description, json_path, json_schema, items_column)
                VALUES (?, ?, ?, ?, ?)
        """,
            [
                name,
                indexable.description,
                indexable.json_path,
                dumps(queryable.json_schema),
                indexable.table_column_name,
            ],
        )
    for collection_id, name in queryables_collections:
        connection.execute(
            """
            INSERT INTO queryables_collections(collection_id, name)
                VALUES (?, ?)
        """,
            [collection_id, name],
        )


def _configure_sortables(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    sortables_collections: List[Tuple[str, str]] = []
    for name, sortable in config.sortables.items():
        indexable = config.indexables[name]
        for collection_id in sortable.collections:
            sortables_collections.append((collection_id, name))
        connection.execute(
            """
                INSERT INTO sortables (name, description, json_path, items_column)
                    VALUES (?, ?, ?, ?)
            """,
            [
                name,
                indexable.description,
                indexable.json_path,
                indexable.table_column_name,
            ],
        )
    for collection_id, name in sortables_collections:
        connection.execute(
            """
            INSERT INTO sortables_collections(collection_id, name)
                VALUES (?, ?)
        """,
            [collection_id, name],
        )
