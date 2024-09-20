from logging import Logger, getLogger
from re import Pattern, compile, match
from time import time
from typing import Final

from aiohttp import ClientSession

from stac_index.common.source_reader import SourceReader

_uri_start_regex: Final[Pattern] = compile(r"^http(s)?://")
_logger: Final[Logger] = getLogger(__file__)


class HttpsSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return not not match(_uri_start_regex, uri)

    def __init__(self):
        super().__init__()
        _logger.info("creating HTTPS Reader")

    async def get_uri_as_string(self, uri: str) -> str:
        try:
            start = time()
            async with ClientSession() as session:
                async with session.get(uri) as response:
                    if response.status == 200:
                        content = await response.text()
                        _logger.debug(
                            "HTTPS: fetched '{}' in {}s".format(
                                uri,
                                round(time() - start, 3),
                            )
                        )
                        return content
                    else:
                        return f"Unable to read '{uri}' ({response.status})"
        except Exception as e:
            raise Exception(f"Unable to read '{uri}'", e)
