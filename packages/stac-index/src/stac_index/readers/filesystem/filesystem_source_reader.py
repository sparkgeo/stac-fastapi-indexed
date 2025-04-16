from glob import glob
from logging import Logger, getLogger
from os import path
from shutil import copy
from time import time
from typing import Final, List, Optional, Tuple

from stac_index.common.exceptions import UriNotFoundException
from stac_index.common.source_reader import SourceReader

_uri_start_str: Final[str] = "/"
_logger: Final[Logger] = getLogger(__name__)


class FilesystemSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return uri.startswith(_uri_start_str)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating Filesystem Reader")

    def path_separator(self) -> str:
        return _uri_start_str  # sorry Windows

    async def get_uri_as_string(self, uri: str) -> str:
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

    async def get_uri_to_file(self, uri: str, file_path: str):
        copy(uri, file_path)

    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        all_uris = glob(
            "{prefix}*".format(prefix=uri if uri.endswith("/") else f"{uri}/")
        )
        return (all_uris[:item_limit], [])
