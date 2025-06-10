from logging import Logger, getLogger
from os import makedirs, path
from shutil import copyfile
from typing import Final, Self

from stac_index.io.filesystem_common import can_handle_uri as can_handle_uri_common
from stac_index.io.filesystem_common import path_separator as path_separator_common
from stac_index.io.writers.source_writer import SourceWriter

_logger: Final[Logger] = getLogger(__name__)


class FilesystemSourceWriter(SourceWriter):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return can_handle_uri_common(uri=uri)

    def __init__(self: Self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating Filesystem Writer")

    def path_separator(self: Self) -> str:
        return path_separator_common()

    async def put_file_to_uri(self: Self, file_path: str, uri: str) -> None:
        _logger.info(f"copying {file_path} to {uri}")
        makedirs(path.dirname(uri), exist_ok=True)
        copyfile(src=file_path, dst=uri)
