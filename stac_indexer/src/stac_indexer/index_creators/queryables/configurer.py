from json import dumps

from duckdb import DuckDBPyConnection

from stac_index_common.queryables import queryable_field_name_to_column_name
from stac_indexer.index_config import IndexConfig


def configure(config: IndexConfig, connection: DuckDBPyConnection) -> None:
    for collection_id, queryables_by_field_name in config.queryables.collection.items():
        for field_name, queryable in queryables_by_field_name.items():
            connection.execute(
                """
                INSERT INTO queryables (collection_id, name, description, json_path, json_schema)
                    VALUES (?, ?, ?, ?, ?)
            """,
                [
                    collection_id,
                    field_name,
                    queryable.description,
                    queryable.json_path,
                    dumps(queryable.json_schema),
                ],
            )
            connection.execute(
                f"""
                ALTER TABLE items
                ADD COLUMN {queryable_field_name_to_column_name(field_name)} {queryable.storage_type.value}
            """
            )
