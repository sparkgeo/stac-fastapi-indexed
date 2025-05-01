from typing import Dict, List, Type

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


_reader_cache: Dict[str, SourceReader] = {}


def get_reader_for_uri(uri: str) -> SourceReader:
    compatible_source_readers = [
        source_reader
        for source_reader in source_reader_classes
        if source_reader.can_handle_uri(uri)
    ]
    if len(compatible_source_readers) == 0:
        raise Exception(f"no source readers support URI '{uri}'")
    reader_class = compatible_source_readers[0]
    if reader_class.__name__ not in _reader_cache:
        _reader_cache[reader_class.__name__] = reader_class()
    return _reader_cache[reader_class.__name__]
