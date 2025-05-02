from glob import glob
from logging import Logger, getLogger
from os import path
from shutil import copy
from time import time
from typing import Final, List, Optional, Self, Tuple

from stac_index.io.filesystem_common import can_handle_uri as can_handle_uri_common
from stac_index.io.filesystem_common import path_separator as path_separator_common
from stac_index.io.readers.exceptions import UriNotFoundException
from stac_index.io.readers.source_reader import SourceReader

_logger: Final[Logger] = getLogger(__name__)


class FilesystemSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return can_handle_uri_common(uri=uri)

    def __init__(self: Self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating Filesystem Reader")

    def path_separator(self: Self) -> str:
        return path_separator_common()

    async def get_uri_as_string(self: Self, uri: str) -> str:
        if path.exists(uri):
            start = time()
            try:
                with open(uri, "r") as f:
                    content = f.read()
                    _logger.debug(
                        "Filesystem: fetched '{}' in {}s".format(
                            uri,
                            round(time() - start, 3),
                        )
                    )
                    return content
            except Exception as e:
                raise Exception(f"Unable to read '{uri}'", e)
        else:
            raise UriNotFoundException(uri)

    async def get_uri_to_file(self: Self, uri: str, file_path: str):
        copy(uri, file_path)

    async def get_item_uris_from_items_uri(
        self: Self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        all_uris = glob(
            "{prefix}*".format(
                prefix=uri
                if uri.endswith(self.path_separator())
                else f"{uri}{self.path_separator()}"
            )
        )
        return (all_uris[:item_limit], [])

    async def get_last_modified_epoch_for_uri(self: Self, uri: str) -> int:
        return round(path.getmtime(filename=uri))
