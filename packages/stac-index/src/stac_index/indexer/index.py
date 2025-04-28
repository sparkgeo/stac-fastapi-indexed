from asyncio import run
from json import load
from logging import Logger, getLogger
from typing import Final, List, Optional, Tuple

from stac_index.common.indexing_error import IndexingError
from stac_index.indexer.creator.creator import IndexCreator
from stac_index.indexer.types.index_config import IndexConfig

_logger: Final[Logger] = getLogger(__name__)


def execute(
    root_catalog_uri: Optional[str] = None,
    manifest_json_uri: Optional[str] = None,
    index_config_path: Optional[str] = None,
):
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
    args = parser.parse_args()
    if args.root_catalog_uri is not None:
        if args.manifest_json_uri is not None:
            raise ValueError(
                f"{root_catalog_uri_key} and {manifest_json_uri_key} are mutually exclusive"
            )
    if args.manifest_json_uri is not None:
        if args.index_config is not None:
            raise ValueError(
                f"{manifest_json_uri_key} and {index_config_key} are mutually exclusive"
            )
    if args.root_catalog_uri is None and args.manifest_json_uri is None:
        raise ValueError(
            f"Either {root_catalog_uri_key} or {manifest_json_uri_key} must be provided"
        )
    execute(
        root_catalog_uri=args.root_catalog_uri,
        manifest_json_uri=args.manifest_json_uri,
        index_config_path=args.index_config,
    )
