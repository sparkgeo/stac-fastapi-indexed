from datetime import datetime, timezone
from glob import glob
from json import dump
from logging import Logger, getLogger
from os import makedirs, path
from typing import Dict, Final, List, Tuple, cast

from duckdb import connect
from shapely.wkt import loads as wkt_loads
from stac_fastapi.types.stac import Collection

from stac_index.indexer.creator.queryables.configurer import (
    configure,
    queryable_field_name_to_column_name,
)
from stac_index.indexer.reader.reader import Reader
from stac_index.indexer.settings import get_settings
from stac_index.indexer.types.index_config import IndexConfig, collection_wildcard
from stac_index.indexer.types.stac_data import ItemWithLocation

_logger: Final[Logger] = getLogger(__file__)


class IndexCreator:
    def __init__(self, index_config: IndexConfig):
        self._index_config = index_config
        self._creation_time = datetime.now(tz=timezone.utc)
        self._conn = connect()
        self._conn.execute("INSTALL spatial")
        self._conn.execute("LOAD spatial")

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    async def process(self, reader: Reader) -> List[str]:
        _logger.info("creating parquet index")
        # may eventually want some logic here to find an update an existing index, for not just creating from scratch each time
        self._create_tables()
        configure(self._index_config, self._conn)
        collections, collection_errors = await self._request_collections(reader)
        items_errors = await self._request_items(reader, collections)
        self._insert_metadata()
        output_dir = path.join(get_settings().output_dir, "parquet")
        try:
            makedirs(output_dir, exist_ok=True)
        except Exception as e:
            _logger.error(
                f"unable to create index destination directory at '{output_dir}'"
            )
            raise e
        # TODO: generate a hash or other unique identifier for a Parquet file collection to guarantee consistency / compatibility between files
        for table_name in [
            row[0] for row in self._conn.execute("SHOW tables").fetchall()
        ]:
            geometry_column_name = None  # assumes max 1 geometry column per table
            for row in self._conn.execute(
                f"SELECT column_name, column_type FROM (DESCRIBE {table_name})"
            ).fetchall():
                column_name, column_type = row
                if cast(str, column_type).upper() == "GEOMETRY":
                    geometry_column_name = column_name
                    break
            if geometry_column_name is None:
                export_select = "*"
            else:
                export_select = "* EXCLUDE ({col}), ST_AsWKB({col}) as {col}".format(
                    col=geometry_column_name
                )
            self._conn.execute(f"""
                COPY (SELECT {export_select} FROM {table_name}) 
                  TO '{output_dir}/{table_name}.parquet'
                  (FORMAT PARQUET)
                ;
            """)
        with open(path.join(output_dir, "manifest.json"), "w") as f:
            dump(
                {
                    "created": self._creation_time.isoformat(),
                },
                f,
                indent=2,
            )
        return collection_errors + items_errors

    def _create_tables(self) -> None:
        sql_directory = path.join(path.dirname(__file__), "sql")
        for sql_path in sorted(
            glob(path.join(sql_directory, "**", "*.sql"), recursive=True)
        ):
            with open(sql_path, "r") as f:
                self._conn.execute(f.read())

    async def _request_collections(
        self, reader: Reader
    ) -> Tuple[List[Collection], List[str]]:
        collections, errors = await reader.get_collections(
            await reader.get_root_catalog()
        )
        for collection in collections:
            insert_sql = """
                INSERT INTO collections (
                    id
                , stac_location
                ) VALUES (
                    ?, ?
                );
            """
            self._conn.execute(insert_sql, (collection.id, collection.location))
        return (collections, errors)

    # Processing items is more complex than collections due to scale.
    # It is possible to have an enormous number of items (collections too, though this is less likely).
    # As a result it may not be sensible to assemble an in-memory list of all items to then iterate over and process.
    # Instead we pass a processor function to the reader so that each item can be processed (i.e. inserted into a table)
    # as it is retrieved.
    async def _request_items(
        self, reader: Reader, collections: List[Collection]
    ) -> List[str]:
        counts: Dict[str, int] = {
            "inserted": 0,
            "invalid": 0,
            "failed": 0,
        }
        insert_fields_and_values_template = {
            "id": "?",
            "collection_id": "?",
            "geometry": "ST_GeomFromText('{geometry_wkt}')",
            "datetime": "?",
            "datetime_end": "?",
            "stac_location": "?",
        }
        for (
            queryables_by_field_name
        ) in self._index_config.queryables.collection.values():
            for field_name in queryables_by_field_name.keys():
                insert_fields_and_values_template[
                    queryable_field_name_to_column_name(field_name)
                ] = "?"

        def processor(item: ItemWithLocation) -> List[str]:
            errors: List[str] = []
            if not wkt_loads(item.geometry.wkt).is_valid:
                errors.append(
                    f"skipping invalid geometry '{item.collection}'/'{item.id}'"
                )
                counts["invalid"] += 1
                return errors

            insert_params = [
                item.id,
                item.collection,
                item.properties.datetime or item.properties.start_datetime,
                item.properties.end_datetime or item.properties.datetime,
                item.location,
            ]
            for (
                collection_id,
                queryables_by_field_name,
            ) in self._index_config.queryables.collection.items():
                for field_name, queryable in queryables_by_field_name.items():
                    insert_param = None
                    if (
                        collection_id == collection_wildcard
                        or collection_id == item.collection
                    ):
                        for path_option in queryable.json_path.split("|"):
                            # where multiple JSON paths are possible accept the first that is not-None
                            path_parts = path_option.split(".")
                            path_parents, path_key = path_parts[:-1], path_parts[-1:][0]
                            param_source = item.to_dict()
                            for parent_part in path_parents:
                                try:
                                    param_source = param_source[parent_part]
                                except KeyError:
                                    break
                            try:
                                insert_param = param_source[path_key]
                                break
                            except KeyError:
                                pass
                    if insert_param is None:
                        errors.append(
                            "could not locate path '{}' for field '{}' in '{}'/'{}'".format(
                                queryable.json_path,
                                field_name,
                                item.collection,
                                item.id,
                            )
                        )
                    insert_params.append(insert_param)
            try:
                self._conn.execute(
                    "INSERT INTO items ({}) VALUES ({})".format(
                        ", ".join(insert_fields_and_values_template.keys()),
                        ", ".join(insert_fields_and_values_template.values()),
                    ).format(geometry_wkt=item.geometry.wkt),
                    insert_params,
                )
                counts["inserted"] += 1
            except Exception as e:
                errors.append(
                    f"failed to insert into '{item.collection}'/'{item.id}': {e}"
                )
                counts["failed"] += 1
            return errors

        errors = await reader.process_items(collections, processor)
        _logger.info(counts)
        return errors

    def _insert_metadata(self) -> None:
        self._conn.execute(
            "INSERT INTO audit (event, time, notes) VALUES (?, ?, ?)",
            (
                "create",
                self._creation_time,
                None,
            ),
        )
