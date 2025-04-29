from typing import List, Type

from .filesystem.filesystem_source_reader import FilesystemSourceReader  # noqa: F401
from .https.https_source_reader import HttpsSourceReader  # noqa: F401
from .s3.s3_source_reader import S3SourceReader  # noqa: F401
from .source_reader import SourceReader

__all__ = [
    "FilesystemSourceReader",
    "HttpsSourceReader",
    "S3SourceReader",
]

source_reader_classes: List[Type[SourceReader]] = [
    FilesystemSourceReader,
    HttpsSourceReader,
    S3SourceReader,
]


def get_reader_class_for_uri(uri: str) -> Type[SourceReader]:
    compatible_source_readers = [
        source_reader
        for source_reader in source_reader_classes
        if source_reader.can_handle_uri(uri)
    ]
    if len(compatible_source_readers) == 0:
        raise Exception(f"no source readers support URI '{uri}'")
    return compatible_source_readers[0]
