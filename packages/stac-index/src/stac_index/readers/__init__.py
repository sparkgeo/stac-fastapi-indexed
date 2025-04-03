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

source_reader_classes = [
    FilesystemSourceReader,
    HttpsSourceReader,
    S3SourceReader,
]
index_reader_classes = [
    FilesystemIndexReader,
    S3IndexReader,
]
