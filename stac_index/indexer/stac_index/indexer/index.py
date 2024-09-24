from asyncio import run
from json import load
from logging import Logger, getLogger
from os import linesep
from typing import Final, List

from stac_index.indexer.creator.creator import IndexCreator
from stac_index.indexer.reader.reader import Reader
from stac_index.indexer.types.index_config import IndexConfig

_logger: Final[Logger] = getLogger(__file__)


def execute(
    index_config_path: str,
):
    with open(index_config_path, "r") as f:
        index_config_dict = load(f)
    index_config = IndexConfig(**index_config_dict)
    errors = run(_call_process(index_config))
    if len(errors) > 0:
        _logger.info(f"Indexing encountered {len(errors)} error(s)")
        _logger.info(linesep.join(errors) + linesep)


async def _call_process(index_config: IndexConfig) -> List[str]:
    return await IndexCreator(index_config=index_config).process(
        Reader(root_catalog_uri=index_config.root_catalog_uri)
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
