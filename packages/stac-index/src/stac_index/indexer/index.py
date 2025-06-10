from asyncio import gather, run
from json import load
from logging import Logger, getLogger
from os import path
from typing import Final, List, Optional, Tuple

from stac_index.indexer.creator.creator import IndexCreator
from stac_index.indexer.types.index_config import IndexConfig
from stac_index.indexer.types.index_manifest import IndexManifest
from stac_index.indexer.types.indexing_error import IndexingError
from stac_index.io.writers import get_writer_for_uri

_logger: Final[Logger] = getLogger(__name__)


def execute(
    root_catalog_uri: Optional[str] = None,
    manifest_json_uri: Optional[str] = None,
    index_config_path: Optional[str] = None,
    publish_uri: Optional[str] = None,
):
    if root_catalog_uri is not None:
        if manifest_json_uri is not None:
            raise ValueError(
                "root_catalog_uri and manifest_json_uri are mutually exclusive"
            )
    if manifest_json_uri is not None:
        if index_config_path is not None:
            raise ValueError(
                "manifest_json_uri and index_config are mutually exclusive"
            )
    if root_catalog_uri is None and manifest_json_uri is None:
        raise ValueError(
            "Either root_catalog_uri or manifest_json_uri must be provided"
        )
    errors, manifest_path = run(
        _call_process(
            root_catalog_uri=root_catalog_uri,
            manifest_json_uri=manifest_json_uri,
            index_config_path=index_config_path,
        )
    )
    if len(errors) > 0:
        _logger.info(
            f"Indexing encountered {len(errors)} error(s). Review errors via API at GET /status/errors"
        )
    _logger.info(manifest_path)
    if publish_uri is not None:
        run(_publish_index(manifest_path=manifest_path, publish_uri=publish_uri))


async def _call_process(
    root_catalog_uri: Optional[str] = None,
    manifest_json_uri: Optional[str] = None,
    index_config_path: Optional[str] = None,
) -> Tuple[List[IndexingError], str]:
    index_creator = IndexCreator()
    if root_catalog_uri is not None:
        if index_config_path is not None:
            with open(index_config_path, "r") as f:
                index_config_dict = load(f)
                index_config = IndexConfig(**index_config_dict)
        else:
            index_config = None
        return await index_creator.create_new_index(
            root_catalog_uri=root_catalog_uri, index_config=index_config
        )
    elif manifest_json_uri is not None:
        return await index_creator.update_index(manifest_json_uri=manifest_json_uri)
    raise Exception("No useable arguments provided")


async def _publish_index(manifest_path: str, publish_uri: str) -> None:
    source_writer = get_writer_for_uri(publish_uri)
    if not publish_uri.endswith(source_writer.path_separator()):
        publish_uri = f"{publish_uri}{source_writer.path_separator()}"
    _logger.info(f"publishing to {publish_uri}")
    with open(manifest_path, "r") as f:
        index_manifest = IndexManifest(**load(f))
    table_uploads = []
    for metadata in index_manifest.tables.values():
        table_file_path = path.join(path.dirname(manifest_path), metadata.relative_path)
        target_uri = "{}{}".format(publish_uri, metadata.relative_path)
        table_uploads.append(source_writer.put_file_to_uri(table_file_path, target_uri))
    await gather(*table_uploads)
    # manifest must go after parquet files so that data is immediately accessible after manifest update
    await source_writer.put_file_to_uri(
        manifest_path, "{}{}".format(publish_uri, path.basename(manifest_path))
    )


if __name__ == "__main__":
    from argparse import ArgumentParser

    root_catalog_uri_key: Final[str] = "--root_catalog_uri"
    manifest_json_uri_key: Final[str] = "--manifest_json_uri"
    index_config_key: Final[str] = "--index_config"
    parser = ArgumentParser()
    parser.add_argument(
        root_catalog_uri_key,
        type=str,
        default=None,
        help=f"Root STAC catalog URI from which a new index should be created. Required *if* {manifest_json_uri_key} is not provided, mutually exclusive",
    )
    parser.add_argument(
        manifest_json_uri_key,
        type=str,
        default=None,
        help=f"URI for existing JSON manifest if updating an existing index. Required *if* {root_catalog_uri_key} is not provided, mutually exclusive",
    )
    parser.add_argument(
        index_config_key,
        type=str,
        default=None,
        help=f"Optional path to an index configuration file if creating a new index. Required for custom indexables, queryables, sortables. Not compatible with {manifest_json_uri_key} (index config cannot be changed in an index update)",
    )
    parser.add_argument(
        "--publish_to_uri",
        type=str,
        default=None,
        help="Optional URI for index publish location",
    )
    args = parser.parse_args()
    execute(
        root_catalog_uri=args.root_catalog_uri,
        manifest_json_uri=args.manifest_json_uri,
        index_config_path=args.index_config,
        publish_uri=args.publish_to_uri,
    )
