from datetime import datetime, timezone
from glob import glob
from json import dump
from logging import Logger, getLogger
from os import makedirs, path
from tempfile import mkdtemp
from typing import Dict, Final, List, Optional, Tuple
from uuid import uuid4

from duckdb import ConstraintException, connect
from shapely import Geometry, is_valid_reason
from shapely.wkt import loads as wkt_loads
from stac_fastapi.types.stac import Collection
from stac_index.common.index_manifest import IndexManifest, TableMetadata
from stac_index.common.indexing_error import (
    IndexingError,
    IndexingErrorType,
    new_error,
    save_error,
)
from stac_index.indexer.creator.configurer import (
    add_items_columns,
    configure_indexables,
)
from stac_index.indexer.reader.reader import Reader
from stac_index.indexer.settings import get_settings
from stac_index.indexer.types.index_config import IndexConfig, collection_wildcard
from stac_index.indexer.types.stac_data import ItemWithLocation
from stac_index.readers import get_index_reader_class_for_uri

_logger: Final[Logger] = getLogger(__name__)
_indexer_version: Final[int] = (
    1  # only increment on changes that are not backwards-compatible
)


def _current_time() -> datetime:
    return datetime.now(tz=timezone.utc)


class IndexCreator:
    def __init__(self):
        self._creation_time = _current_time()
        self._conn = connect()
        self._conn.execute("INSTALL spatial")
        self._conn.execute("LOAD spatial")

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def create_empty(self, output_dir: Optional[str] = None) -> str:
        _logger.info("creating empty index")
        self._create_db_objects()
        return self._export_db_objects(output_dir or get_settings().output_dir)

    async def index_stac_source(
        self,
        index_config: IndexConfig,
        reader: Reader,
        output_dir: Optional[str] = None,
    ) -> Tuple[List[IndexingError], str]:
        load_id: Final[str] = str(uuid4())
        _logger.info(f"indexing stac source for load {load_id}")
        await self._load_existing_data(index_config=index_config)
        self._create_db_objects()
        add_items_columns(index_config, self._conn)
        collections, collection_errors = await self._request_collections(reader)
        items_errors = await self._request_items(index_config, reader, collections)
        configure_indexables(index_config, self._conn)
        self._log_index_event(load_id=load_id, index_config=index_config)
        return (
            collection_errors + items_errors,
            self._export_db_objects(output_dir or get_settings().output_dir),
        )

    def _create_db_objects(self) -> None:
        sql_directory = path.join(path.dirname(__file__), "sql")
        for sql_path in sorted(
            glob(path.join(sql_directory, "**", "*.sql"), recursive=True)
        ):
            with open(sql_path, "r") as f:
                try:
                    self._conn.execute(f.read())
                except Exception:
                    _logger.exception(f"SQL failure at {sql_path}")
                    raise

    def _export_db_objects(self, output_dir: str) -> str:
        manifest = IndexManifest(
            indexer_version=_indexer_version,
            created=self._creation_time,
        )
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
            if table_name not in [
                "collections",
                "items",
                "queryables_by_collection",
                "sortables_by_collection",
                "errors",
            ]:
                continue
            output_filename = f"{table_name}.parquet"
            self._conn.execute(f"""
                COPY (SELECT * FROM {table_name})
                  TO '{output_dir}/{output_filename}'
                  (FORMAT PARQUET)
                ;
            """)
            manifest.tables[table_name] = TableMetadata(
                relative_path=output_filename,
            )
        manifest_path = path.join(output_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            dump(
                manifest.model_dump(),
                f,
                indent=2,
            )
        return manifest_path

    async def _request_collections(
        self, reader: Reader
    ) -> Tuple[List[Collection], List[IndexingError]]:
        collections, errors = await reader.get_collections(
            await reader.get_root_catalog()
        )
        _logger.info(f"discovered {len(collections)} collection(s)")
        for collection in collections:
            insert_sql = """
                INSERT INTO collections (
                    id
                , stac_location
                ) VALUES (
                    ?, ?
                );
            """
            try:
                self._conn.execute(insert_sql, (collection.id, collection.location))
            except Exception as e:
                errors.append(
                    new_error(
                        IndexingErrorType.unknown,
                        f"failed to insert collection '{collection.id}': {e}",
                        collection=collection.id,
                    )
                )
        return (collections, errors)

    # Processing items is more complex than collections due to scale.
    # It is possible to have an enormous number of items (collections too, though this is less likely).
    # As a result it may not be sensible to assemble an in-memory list of all items to then iterate over and process.
    # Instead we pass a processor function to the reader so that each item can be processed (i.e. inserted into a table)
    # as it is retrieved.
    async def _request_items(
        self, index_config: IndexConfig, reader: Reader, collections: List[Collection]
    ) -> List[IndexingError]:
        counts: Dict[str, int] = {
            "inserted": 0,
            "invalid": 0,
            "failed": 0,
            "duplicates": 0,
        }
        insert_fields_and_values_template = {
            "id": "?",
            "collection_id": "?",
            "geometry": "ST_GeomFromText('{geometry_wkt}')",
            "datetime": "?",
            "start_datetime": "?",
            "end_datetime": "?",
            "stac_location": "?",
            "applied_fixes": "?",
        }
        for indexable in index_config.indexables.values():
            insert_fields_and_values_template[indexable.table_column_name] = "?"

        def processor(item: ItemWithLocation) -> List[IndexingError]:
            errors: List[IndexingError] = []
            geometry: Geometry = wkt_loads(item.geometry.wkt)
            if not geometry.is_valid:
                errors.append(
                    new_error(
                        IndexingErrorType.item_validation,
                        f"Invalid geometry for '{item.collection}'/'{item.id}': {is_valid_reason(geometry)}",
                        subtype="invalid_geometry",
                        collection=item.collection,
                        item=item.id,
                    )
                )
                counts["invalid"] += 1
                return errors

            insert_params = [
                item.id,
                item.collection,
                item.properties.datetime,
                item.properties.start_datetime,
                item.properties.end_datetime,
                item.location,
                ",".join(item.applied_fixes)
                if item.applied_fixes is not None
                else "NONE",
            ]
            for (
                collection_id,
                indexable_by_field_name,
            ) in index_config.all_indexables_by_collection.items():
                if (
                    collection_id == collection_wildcard
                    or collection_id == item.collection
                ):
                    for field_name, indexable in indexable_by_field_name.items():
                        insert_param = None
                        for path_option in indexable.json_path.split("|"):
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
                                new_error(
                                    IndexingErrorType.item_validation,
                                    "could not locate path '{}' for field '{}' in '{}'/'{}'".format(
                                        indexable.json_path,
                                        field_name,
                                        item.collection,
                                        item.id,
                                    ),
                                    collection=item.collection,
                                    item=item.id,
                                )
                            )
                        else:
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
            except ConstraintException as e:
                if "duplicate key" in str(e).lower():
                    errors.append(
                        new_error(
                            IndexingErrorType.item_validation,
                            f"duplicate in '{item.collection}'/'{item.id}'",
                            collection=item.collection,
                            item=item.id,
                        )
                    )
                    counts["duplicates"] += 1
                else:
                    raise  # defer to generic handler
            except Exception as e:
                errors.append(
                    new_error(
                        IndexingErrorType.unknown,
                        f"failed to insert into '{item.collection}'/'{item.id}': {e}",
                        collection=item.collection,
                        item=item.id,
                    )
                )
                counts["failed"] += 1
            return errors

        errors = await reader.process_items(collections, processor)
        self._insert_errors(errors)
        _logger.info(counts)
        return errors

    def _log_index_event(self, load_id: str, index_config: IndexConfig) -> None:
        self._conn.execute(
            "INSERT INTO index_history (id, start_time, end_time, root_catalog_uris, loaded, added, removed, updated, unchanged) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                load_id,
                self._creation_time,
                _current_time(),
                [index_config.root_catalog_uri],
                0,
                0,
                0,
                0,
                0,
            ),
        )

    def _insert_errors(self, errors: list[IndexingError]) -> None:
        for error in errors:
            save_error(self._conn, error)

    async def _load_existing_data(self, index_config: IndexConfig) -> None:
        if index_config.existing_manifest_json_uri is None:
            return
        index_reader = get_index_reader_class_for_uri(
            index_config.existing_manifest_json_uri
        )(index_config.existing_manifest_json_uri)
        index_manifest = await index_reader.get_index_manifest()
        if index_manifest.indexer_version != _indexer_version:
            raise Exception(
                f"indexer v{_indexer_version} incompatible with manifest from v{index_manifest.indexer_version}"
            )
        tmp_dir_path = mkdtemp()
        for table_name in ["collections", "items"]:
            if table_name not in index_manifest.tables:
                raise ValueError(f"{table_name} table not present in index_manifest")
            relative_path = index_manifest.tables[table_name].relative_path
            tmp_file_path = path.join(tmp_dir_path, relative_path)
            source_reader = index_reader.source_reader
            await source_reader.get_uri_to_file(
                source_reader.path_separator().join(
                    index_config.existing_manifest_json_uri.split(
                        source_reader.path_separator()
                    )[:-1]
                    + [relative_path]
                ),
                tmp_file_path,
            )
            previous_table_name = f"{table_name}_previous"
            _logger.info(f"creating {previous_table_name} from {tmp_file_path}")
            self._conn.execute(
                f"CREATE TABLE {previous_table_name} AS SELECT * FROM '{tmp_file_path}'"
            )
