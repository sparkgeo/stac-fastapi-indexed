from asyncio import run
from json import load
from logging import Logger, getLogger
from typing import Final, List

from stac_index.common.indexing_error import IndexingError
from stac_index.indexer.creator.creator import IndexCreator
from stac_index.indexer.reader.reader import Reader
from stac_index.indexer.types.index_config import IndexConfig

_logger: Final[Logger] = getLogger(__name__)


def execute(
    index_config_path: str,
):
    with open(index_config_path, "r") as f:
        index_config_dict = load(f)
    index_config = IndexConfig(**index_config_dict)
    errors = run(_call_process(index_config))
    if len(errors) > 0:
        _logger.info(
            f"Indexing encountered {len(errors)} error(s). Review errors via API at GET /status/errors"
        )


async def _call_process(index_config: IndexConfig) -> List[IndexingError]:
    return await IndexCreator(index_config=index_config).create_and_populate(
        Reader(
            root_catalog_uri=index_config.root_catalog_uri,
            fixes_to_apply=index_config.fixes_to_apply,
        )
    )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "index_config",
        type=str,
        help="Path to the index configuration file",
    )
    args = parser.parse_args()
    execute(
        index_config_path=args.index_config,
    )
