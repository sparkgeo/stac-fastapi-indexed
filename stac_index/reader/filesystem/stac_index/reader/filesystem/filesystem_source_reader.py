from glob import glob
from logging import Logger, getLogger
from os import path
from time import time
from typing import Final, List, Optional, Tuple

from stac_index.common.source_reader import SourceReader

_uri_start_str: Final[str] = "/"
_logger: Final[Logger] = getLogger(__file__)


class FilesystemSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return uri.startswith(_uri_start_str)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating Filesystem Reader")

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
            raise ValueError(f"'{uri}' does not exist")

    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        all_uris = glob(
            "{prefix}*".format(prefix=uri if uri.endswith("/") else f"{uri}/")
        )
        return (all_uris[:item_limit], [])
