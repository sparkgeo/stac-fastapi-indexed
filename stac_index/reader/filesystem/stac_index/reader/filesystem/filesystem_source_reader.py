from glob import glob
from logging import Logger, getLogger
from os import path
from time import time
from typing import Final, List, Optional

from stac_index.common.source_reader import SourceReader

_uri_start_str: Final[str] = "/"
_logger: Final[Logger] = getLogger(__file__)


class FilesystemSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return uri.startswith(_uri_start_str)

    def __init__(self):
        super().__init__()
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

    async def list_uris_by_prefix(
        self,
        uri_prefix: str,
        list_limit: Optional[int] = None,
        uri_suffix: Optional[str] = None,
    ) -> List[str]:
        all_uris = glob(
            "{prefix}*{suffix}".format(
                prefix=uri_prefix if uri_prefix.endswith("/") else f"{uri_prefix}/",
                suffix="" if uri_suffix is None else uri_suffix,
            )
        )
        if list_limit is not None:
            return all_uris[:list_limit]
        return all_uris
