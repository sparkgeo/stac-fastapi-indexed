from json import dumps

from duckdb import DuckDBPyConnection

from stac_index.indexer.types.index_config import IndexConfig


def configure(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    _configure_queryables(config, connection)
    _configure_sortables(config, connection)


def _configure_queryables(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    for collection_id, queryables_by_field_name in config.queryables.collection.items():
        for field_name, queryable in queryables_by_field_name.items():
            connection.execute(
                """
                INSERT INTO queryables (collection_id, name, description, json_path, json_schema, items_column, is_geometry, is_temporal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    collection_id,
                    field_name,
                    queryable.description,
                    queryable.json_path,
                    dumps(queryable.json_schema),
                    queryable_field_name_to_column_name(field_name),
                    queryable.is_geometry,
                    queryable.is_temporal,
                ],
            )
            connection.execute(
                f"""
                ALTER TABLE items
                ADD COLUMN {queryable_field_name_to_column_name(field_name)} {queryable.storage_type.value}
            """
            )


def _configure_sortables(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    for collection_id, sortables_by_field_name in config.sortables.collection.items():
        for field_name, sortable in sortables_by_field_name.items():
            connection.execute(
                """
                INSERT INTO sortables (collection_id, name, description, items_column)
                    VALUES (?, ?, ?, ?)
            """,
                [
                    collection_id,
                    field_name,
                    sortable.description,
                    queryable_field_name_to_column_name(field_name),
                ],
            )


def queryable_field_name_to_column_name(field_name: str) -> str:
    return f"q_{field_name}"
