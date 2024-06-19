from json import dumps

from duckdb import DuckDBPyConnection

from stac_indexer.index_config import IndexConfig
from stac_indexer.index_creators.queryables.fields import (
    queryable_field_name_to_column_name,
)


def configure(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    for collection_id, queryables_by_field_name in config.queryables.collection.items():
        for field_name, queryable in queryables_by_field_name.items():
            connection.execute(
                """
                INSERT INTO queryables (collection_id, name, description, json_path, json_schema, items_column)
                    VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    collection_id,
                    field_name,
                    queryable.description,
                    queryable.json_path,
                    dumps(queryable.json_schema),
                    queryable_field_name_to_column_name(field_name),
                ],
            )
            connection.execute(
                f"""
                ALTER TABLE items
                ADD COLUMN {queryable_field_name_to_column_name(field_name)} {queryable.storage_type.value}
            """
            )
