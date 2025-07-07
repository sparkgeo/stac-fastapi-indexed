from datetime import datetime, timezone
from glob import glob
from hashlib import md5
from json import dump
from logging import Logger, getLogger
from os import makedirs, path
from tempfile import mkdtemp
from typing import Dict, Final, List, Optional, Self, Tuple, cast
from uuid import uuid4

from duckdb import ConstraintException, connect
from shapely import Geometry, is_valid_reason
from shapely.wkt import loads as wkt_loads
from stac_fastapi.types.stac import Collection
from stac_index.indexer.creator.configurer import (
    add_items_columns,
    configure_indexables,
)
from stac_index.indexer.stac_catalog_reader import StacCatalogReader
from stac_index.indexer.types.index_config import IndexConfig, collection_wildcard
from stac_index.indexer.types.index_manifest import IndexManifest, TableMetadata
from stac_index.indexer.types.indexing_error import (
    IndexingError,
    IndexingErrorType,
    new_error,
    save_error,
)
from stac_index.indexer.types.stac_data import ItemWithLocation
from stac_index.io.readers import get_reader_for_uri

_logger: Final[Logger] = getLogger(__name__)
_indexer_version: Final[int] = (
    1  # only increment on changes that are not backwards-compatible
)


def _current_time() -> datetime:
    return datetime.now(tz=timezone.utc)


class IndexCreator:
    def __init__(self: Self):
        self._creation_time = _current_time()
        self._conn = connect()
        self._conn.execute("INSTALL spatial")
        self._conn.execute("LOAD spatial")
        self._load_id = uuid4().hex

    def __del__(self: Self):
        try:
            self._conn.close()
        except Exception:
            pass

    def create_empty(self: Self) -> str:
        _logger.info("creating empty index")
        self._create_db_objects()
        return self._export_db_objects()

    async def create_new_index(
        self: Self,
        root_catalog_uri: str,
        index_config: Optional[IndexConfig] = None,
    ) -> Tuple[List[IndexingError], str]:
        return await self._index_stac_source(
            root_catalog_uri=root_catalog_uri, index_config=index_config
        )

    async def update_index(
        self: Self,
        manifest_json_uri: str,
    ) -> Tuple[List[IndexingError], str]:
        existing_index_manifest = await self._load_existing_index(manifest_json_uri)
        return await self._index_stac_source(
            root_catalog_uri=cast(str, existing_index_manifest.root_catalog_uri),
            index_config=existing_index_manifest.index_config,
        )

    async def _index_stac_source(
        self: Self,
        root_catalog_uri: str,
        index_config: Optional[IndexConfig] = None,
        output_dir: Optional[str] = None,
    ) -> Tuple[List[IndexingError], str]:
        _logger.info(f"indexing stac source for load {self._load_id}")
        self._create_db_objects()
        index_config = index_config or IndexConfig()
        add_items_columns(index_config, self._conn)
        reader = StacCatalogReader(
            root_catalog_uri=root_catalog_uri,
            fixes_to_apply=index_config.fixes_to_apply,
        )
        collections, collection_errors = await self._request_collections(reader)
        items_errors = await self._request_items(index_config, reader, collections)
        configure_indexables(index_config, self._conn)
        self._log_index_event(root_catalog_uri=root_catalog_uri)
        return (
            collection_errors + items_errors,
            self._export_db_objects(
                root_catalog_uri=root_catalog_uri,
            ),
        )

    def _create_db_objects(self: Self) -> None:
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

    def _export_db_objects(
        self: Self,
        root_catalog_uri: Optional[str] = None,
        index_config: Optional[IndexConfig] = None,
    ) -> str:
        output_relative_dir = path.join(
            "{}-{}".format(
                self._creation_time.strftime("%Y-%m-%dT%H.%M.%S.%fZ"),
                self._load_id,
            )
        )
        output_base_dir = mkdtemp()
        output_dir = path.join(output_base_dir, output_relative_dir)
        try:
            makedirs(output_dir, exist_ok=True)
        except Exception:
            _logger.exception(
                f"unable to create index destination directory at '{output_dir}'"
            )
            raise
        manifest = IndexManifest(
            indexer_version=_indexer_version,
            updated=self._creation_time,
            load_id=self._load_id,
            root_catalog_uri=root_catalog_uri,
            index_config=index_config,
        )
        for table_name in [
            row[0] for row in self._conn.execute("SHOW tables").fetchall()
        ]:
            if table_name not in [
                "collections",
                "items",
                "queryables_by_collection",
                "sortables_by_collection",
                "errors",
                "index_history",
            ]:
                continue
            table_filename = f"{table_name}.parquet"
            self._conn.execute(f"""
                COPY (SELECT * FROM {table_name})
                  TO '{path.join(output_dir, table_filename)}'
                  (FORMAT PARQUET)
                ;
            """)
            manifest.tables[table_name] = TableMetadata(
                relative_path=path.join(output_relative_dir, table_filename),
            )
        manifest_path = path.join(output_base_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            dump(
                manifest.model_dump(),
                f,
                indent=2,
            )
        return manifest_path

    async def _request_collections(
        self: Self, reader: StacCatalogReader
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
                , load_id
                , collection_hash
                ) VALUES (
                    ?, ?, ?, ?
                );
            """
            try:
                self._conn.execute(
                    insert_sql,
                    (
                        collection.id,
                        collection.location,
                        self._load_id,
                        self._hash_data(collection.to_json()),
                    ),
                )
            except Exception as e:
                errors.append(
                    new_error(
                        IndexingErrorType.unknown,
                        f"failed to insert collection '{collection.id}': {e}",
                        collection=collection.id,
                    )
                )
        self._insert_errors(errors)
        return (collections, errors)

    # Processing items is more complex than collections due to scale.
    # It is possible to have an enormous number of items (collections too, though this is less likely).
    # As a result it may not be sensible to assemble an in-memory list of all items to then iterate over and process.
    # Instead we pass a processor function to the reader so that each item can be processed (i.e. inserted into a table)
    # as it is retrieved.
    async def _request_items(
        self: Self,
        index_config: IndexConfig,
        reader: StacCatalogReader,
        collections: List[Collection],
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
            "load_id": "?",
            "item_hash": "?",
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
                self._load_id,
                self._hash_data(item.to_json()),
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

    def _log_index_event(self: Self, root_catalog_uri: str) -> None:
        insert_sql_template = """
        INSERT INTO index_history (
            id
            , start_time
            , end_time
            , root_catalog_uris
            , items_loaded
            , items_added
            , items_removed
            , items_updated
            , items_unchanged
            , collections_loaded
            , collections_added
            , collections_removed
            , collections_updated
            , collections_unchanged
        )
        VALUES (?, ?, ?, ?, {items_loaded}, {items_added}, {items_removed}, {items_updated}, {items_unchanged}, {collections_loaded}, {collections_added}, {collections_removed}, {collections_updated}, {collections_unchanged})
        """
        insert_sql_args = (
            self._load_id,
            self._creation_time,
            _current_time(),
            [root_catalog_uri],
        )
        history_tables = (
            "items_previous",
            "collections_previous",
            "index_history_previous",
        )
        has_history = self._conn.execute(
            f"""
            SELECT COUNT(*) = {len(history_tables)}
              FROM duckdb_tables() t
        INNER JOIN UNNEST(['{"', '".join(history_tables)}']) AS vals(expected_table_name)
                ON t.table_name = vals.expected_table_name
        """
        ).fetchone()[0]
        if has_history:
            self._conn.execute(
                """
                INSERT INTO index_history
                SELECT *
                  FROM index_history_previous
                """
            )
            # these statements will need updating if we support multi-catalog indexing where individual catalogs can be updated separately
            # right now we can assume that the items and collections tables represent all source data and therefore don't need to check load IDs or catalogs
            insert_sql = insert_sql_template.format(
                items_loaded="(SELECT COUNT(*) FROM items)",
                items_added=""" (
                SELECT COUNT(*)
                  FROM items
                 WHERE CONCAT(collection_id, '.', id) NOT IN (SELECT CONCAT(collection_id, '.', id) FROM items_previous)
                ) """,
                items_removed=""" (
                SELECT COUNT(*)
                  FROM items_previous
                 WHERE CONCAT(collection_id, '.', id) NOT IN (SELECT CONCAT(collection_id, '.', id) FROM items)
                ) """,
                items_updated=""" (
                SELECT COUNT(*)
                  FROM items i
            INNER JOIN items_previous ip ON CONCAT(i.collection_id, '.', i.id) = CONCAT(ip.collection_id, '.', ip.id)
                   AND i.item_hash != ip.item_hash
                ) """,
                items_unchanged=""" (
                SELECT COUNT(*)
                  FROM items i
            INNER JOIN items_previous ip ON CONCAT(i.collection_id, '.', i.id) = CONCAT(ip.collection_id, '.', ip.id)
                   AND i.item_hash = ip.item_hash
                ) """,
                collections_loaded="(SELECT COUNT(*) FROM collections)",
                collections_added=""" (
                SELECT COUNT(*)
                  FROM collections
                 WHERE id NOT IN (SELECT id FROM collections_previous)
                ) """,
                collections_removed=""" (
                SELECT COUNT(*)
                  FROM collections_previous
                 WHERE id NOT IN (SELECT id FROM collections)
                ) """,
                collections_updated=""" (
                SELECT COUNT(*)
                  FROM collections c
            INNER JOIN collections_previous cp ON c.id = cp.id
                   AND c.collection_hash != cp.collection_hash
                ) """,
                collections_unchanged=""" (
                SELECT COUNT(*)
                  FROM collections c
            INNER JOIN collections_previous cp ON c.id = cp.id
                   AND c.collection_hash = cp.collection_hash
                ) """,
            )
        else:
            insert_sql = insert_sql_template.format(
                items_loaded="(SELECT COUNT(*) FROM items)",
                items_added="(SELECT COUNT(*) FROM items)",
                items_removed=0,
                items_updated=0,
                items_unchanged=0,
                collections_loaded="(SELECT COUNT(*) FROM collections)",
                collections_added="(SELECT COUNT(*) FROM collections)",
                collections_removed=0,
                collections_updated=0,
                collections_unchanged=0,
            )
        try:
            self._conn.execute(insert_sql, insert_sql_args)
        except ConstraintException:
            _logger.exception(
                "invalid load counts. ?_loaded is not the sum of all other counts"
            )
            raise

    def _insert_errors(self: Self, errors: list[IndexingError]) -> None:
        for error in errors:
            try:
                save_error(self._conn, error)
            except Exception as e:
                _logger.exception("failed to insert indexing error: {}".format(e))

    async def _load_existing_index(self: Self, manifest_json_uri: str) -> IndexManifest:
        source_reader = get_reader_for_uri(uri=manifest_json_uri)
        index_reader = source_reader.get_index_reader(
            index_manifest_uri=manifest_json_uri
        )
        index_manifest = await index_reader.get_index_manifest()
        if index_manifest.indexer_version != _indexer_version:
            raise Exception(
                f"indexer v{_indexer_version} incompatible with manifest from v{index_manifest.indexer_version}"
            )
        tmp_dir_path = mkdtemp()
        for table_name in ["collections", "items", "index_history"]:
            if table_name not in index_manifest.tables:
                raise ValueError(f"{table_name} table not present in index_manifest")
            relative_path = index_manifest.tables[table_name].relative_path
            tmp_file_path = path.join(tmp_dir_path, relative_path)
            makedirs(path.dirname(tmp_file_path), exist_ok=True)
            await source_reader.get_uri_to_file(
                source_reader.path_separator().join(
                    manifest_json_uri.split(source_reader.path_separator())[:-1]
                    + [relative_path]
                ),
                tmp_file_path,
            )
            previous_table_name = f"{table_name}_previous"
            _logger.info(f"creating {previous_table_name} from {tmp_file_path}")
            self._conn.execute(
                f"CREATE TABLE {previous_table_name} AS SELECT * FROM '{tmp_file_path}'"
            )
        return index_manifest

    def _hash_data(self: Self, data_str: str) -> str:
        return md5(data_str.encode()).hexdigest()
