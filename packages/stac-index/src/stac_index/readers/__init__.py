from typing import List, Type

from ..common.index_reader import IndexReader
from ..common.source_reader import SourceReader
from .filesystem.filesystem_index_reader import FilesystemIndexReader  # noqa: F401
from .filesystem.filesystem_source_reader import FilesystemSourceReader  # noqa: F401
from .https.https_source_reader import HttpsSourceReader  # noqa: F401
from .s3.s3_index_reader import S3IndexReader  # noqa: F401
from .s3.s3_source_reader import S3SourceReader  # noqa: F401

__all__ = [
    "FilesystemIndexReader",
    "FilesystemSourceReader",
    "HttpsSourceReader",
    "S3IndexReader",
    "S3SourceReader",
]

source_reader_classes: List[Type[SourceReader]] = [
    FilesystemSourceReader,
    HttpsSourceReader,
    S3SourceReader,
]
index_reader_classes: List[Type[IndexReader]] = [
    FilesystemIndexReader,
    S3IndexReader,
]


def get_index_reader_class_for_uri(uri: str) -> Type[IndexReader]:
    compatible_index_readers = [
        index_reader
        for index_reader in index_reader_classes
        if index_reader.can_handle_source_uri(uri)
    ]
    if len(compatible_index_readers) == 0:
        raise Exception(f"no index readers support manifest URI '{uri}'")
    return compatible_index_readers[0]
