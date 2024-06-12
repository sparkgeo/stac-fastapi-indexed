from datetime import datetime, timezone
from glob import glob
from json import dump
from logging import Logger, getLogger
from os import makedirs, path
from typing import Dict, Final, cast

from duckdb import connect
from shapely.wkt import loads as wkt_loads

from stac_indexer.index_creators.index_creator import IndexCreator
from stac_indexer.settings import get_settings
from stac_indexer.types.stac_data import StacData

_logger: Final[Logger] = getLogger(__file__)


class ParquetIndexCreator(IndexCreator):
    def __init__(self, stac_data: StacData):
        self._stac_data = stac_data
        self._creation_time = datetime.now(tz=timezone.utc)
        self._conn = connect()
        self._conn.execute("INSTALL spatial")
        self._conn.execute("LOAD spatial")

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    @classmethod
    def create_index_creator(cls, stac_data: StacData) -> "IndexCreator":
        return cls(stac_data=stac_data)

    def process(self) -> str:
        _logger.info("creating parquet index")
        # may eventually want some logic here to find an update an existing index, for not just creating from scratch each time
        self._create_tables()
        self._insert_collections()
        self._insert_items()
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
        for table_name in (
            "collections",
            "items",
            "access",
            "access_properties",
            "audit",
        ):
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
                    "collections": len(self._stac_data.collections),
                    "items": len(self._stac_data.items),
                },
                f,
                indent=2,
            )
        return output_dir

    def _create_tables(self) -> None:
        sql_directory = path.join(path.dirname(__file__), "sql")
        for sql_path in sorted(
            glob(path.join(sql_directory, "**", "*.sql"), recursive=True)
        ):
            with open(sql_path, "r") as f:
                self._conn.execute(f.read())

    def _insert_collections(self) -> None:
        insert_sql = """
            INSERT INTO collections (
                id
              , stac_location
            ) VALUES (
                ?, ?
            );
        """
        for insert_values in [
            (collection.id, collection.location)
            for collection in self._stac_data.collections
        ]:
            self._conn.execute(insert_sql, insert_values)

    def _insert_items(self) -> None:
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
        counts: Dict[str, int] = {
            "inserted": 0,
            "invalid": 0,
        }
        for item in self._stac_data.items:
            if not wkt_loads(item.geometry.wkt).is_valid:
                _logger.debug(
                    f"skipping invalid geometry '{item.collection}'/'{item.id}'"
                )
                counts["invalid"] += 1
                continue
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
                _logger.warn(
                    f"failed to insert into '{item.collection}'/'{item.id}': {e}"
                )
        _logger.info(counts)

    def _insert_metadata(self) -> None:
        self._conn.execute(
            "INSERT INTO access (method) VALUES (?);",
            (self._stac_data.data_access_type.value,),
        )
        self._conn.execute(
            "INSERT INTO audit (event, time, notes) VALUES (?, ?, ?)",
            (
                "create",
                self._creation_time,
                None,
            ),
        )
