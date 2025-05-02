from typing import Dict, List, Type

from .filesystem.filesystem_source_writer import FilesystemSourceWriter
from .s3.s3_source_writer import S3SourceWriter
from .source_writer import SourceWriter

__all__ = [
    "FilesystemSourceWriter",
    "S3SourceWriter",
]

source_writer_classes: List[Type[SourceWriter]] = [
    FilesystemSourceWriter,
    S3SourceWriter,
]


_writer_cache: Dict[str, SourceWriter] = {}


def get_writer_for_uri(uri: str) -> SourceWriter:
    compatible_source_writers = [
        source_writer
        for source_writer in source_writer_classes
        if source_writer.can_handle_uri(uri)
    ]
    if len(compatible_source_writers) == 0:
        raise Exception(f"no source writers support URI '{uri}'")
    writer_class = compatible_source_writers[0]
    if writer_class.__name__ not in _writer_cache:
        _writer_cache[writer_class.__name__] = writer_class()
    return _writer_cache[writer_class.__name__]
