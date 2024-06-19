from json import load
from os import linesep
from typing import Final, List, Type

from stac_indexer.index_config import IndexConfig
from stac_indexer.index_creators.index_creator import IndexCreator
from stac_indexer.readers.reader import Reader

# from stac_indexer.readers.local_file_reader.local_file_reader import LocalFileReader
from stac_indexer.readers.s3_reader.s3_reader import S3Reader

_readers: Final[List[Type[Reader]]] = [
    # LocalFileReader,
    S3Reader,
]


def execute(
    index_config_path: str,
):
    with open(index_config_path, "r") as f:
        index_config_dict = load(f)
    index_config = IndexConfig(**index_config_dict)
    available_readers = [
        reader
        for reader in [
            reader.create_reader(index_config.root_catalog_url) for reader in _readers
        ]
        if reader is not None
    ]
    if len(available_readers) == 0:
        raise Exception(f"No readers able to handle {index_config.root_catalog_url}")
    errors = IndexCreator(index_config=index_config).process(available_readers[0])
    if len(errors) > 0:
        print(linesep.join(errors) + linesep)


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
