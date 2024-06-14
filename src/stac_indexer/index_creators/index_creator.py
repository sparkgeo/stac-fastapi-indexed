from datetime import datetime, timezone
from glob import glob
from json import dump
from logging import Logger, getLogger
from os import makedirs, path
from typing import Dict, Final, List, Tuple, cast

from duckdb import connect
from shapely.wkt import loads as wkt_loads
from stac_fastapi.types.stac import Collection

from stac_indexer.readers.reader import Reader
from stac_indexer.settings import get_settings
from stac_indexer.types.stac_data import ItemWithLocation

_logger: Final[Logger] = getLogger(__file__)


class IndexCreator:
    def __init__(self):
        self._creation_time = datetime.now(tz=timezone.utc)
        self._conn = connect()
        self._conn.execute("INSTALL spatial")
        self._conn.execute("LOAD spatial")

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def process(self, reader: Reader) -> List[str]:
        _logger.info("creating parquet index")
        # may eventually want some logic here to find an update an existing index, for not just creating from scratch each time
        self._create_tables()
        collections, collection_errors = self._request_collections(reader)
        items_errors = self._request_items(reader, collections)
        self._insert_metadata()
        output_dir = path.join(get_settings().index_output_dir, "parquet")
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

    def _request_collections(
        self, reader: Reader
    ) -> Tuple[List[Collection], List[str]]:
        collections, errors = reader.get_collections(reader.get_root_catalog())
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
    def _request_items(
        self, reader: Reader, collections: List[Collection]
    ) -> List[str]:
        counts: Dict[str, int] = {
            "inserted": 0,
            "invalid": 0,
            "failed": 0,
        }

        def processor(item: ItemWithLocation) -> List[str]:
            errors: List[str] = []
            insert_sql_template = """
                INSERT INTO items (
                    id
                , collection_id
                , geometry
                , datetime
                , datetime_end
                , cloud_cover
                , stac_location
                ) VALUES (
                    ?, ?, {geom}, ?, ?, ?, ?
                );
            """
            if not wkt_loads(item.geometry.wkt).is_valid:
                errors.append(
                    f"skipping invalid geometry '{item.collection}'/'{item.id}'"
                )
                counts["invalid"] += 1
                return errors
            insert_sql = insert_sql_template.format(
                geom=f"ST_GeomFromText('{item.geometry.wkt}')"
            )
            try:
                self._conn.execute(
                    insert_sql,
                    (
                        item.id,
                        item.collection,
                        item.properties.datetime or item.properties.start_datetime,
                        item.properties.end_datetime,
                        getattr(item, "eo:cloud_cover", None),
                        item.location,
                    ),
                )
                counts["inserted"] += 1
            except Exception as e:
                errors.append(
                    f"failed to insert into '{item.collection}'/'{item.id}': {e}"
                )
                counts["failed"] += 1
            return errors

        errors = reader.process_items(collections, processor)
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
