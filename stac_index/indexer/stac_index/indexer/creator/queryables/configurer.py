from json import dumps

from duckdb import DuckDBPyConnection

from stac_index.indexer.types.index_config import IndexConfig


def configure(config: IndexConfig, connection: DuckDBPyConnection) -> None:
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


def queryable_field_name_to_column_name(field_name: str) -> str:
    return f"q_{field_name}"
