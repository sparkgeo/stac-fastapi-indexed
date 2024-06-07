from os import linesep
from typing import Final, List, Type

from stac_indexer.readers.local_file_reader.local_file_reader import LocalFileReader
from stac_indexer.readers.reader import Reader

_readers: Final[List[Type[Reader]]] = [
    LocalFileReader,
]


def execute(
    root_catalog_url: str,
):
    available_readers = [
        reader
        for reader in [reader.create_reader(root_catalog_url) for reader in _readers]
        if reader is not None
    ]
    if len(available_readers) == 0:
        raise Exception(f"No readers able to handle {root_catalog_url}")
    if len(available_readers) > 0:
        reader = available_readers[0]
        _, errors = reader.process()
        if len(errors) > 0:
            print(linesep.join(errors) + linesep)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "root_catalog_url",
        type=str,
        help="URL for the root of the STAC catalog. Prefix with file://, s3://, https://, etc.",
    )
    args = parser.parse_args()
    execute(
        root_catalog_url=args.root_catalog_url,
    )
