from logging import Logger, getLogger
from re import Pattern, compile, match
from time import time
from typing import Any, Dict, Final, List, Optional, Tuple, cast

from aiohttp import ClientSession
from stac_index.common.exceptions import UriNotFoundException
from stac_index.common.source_reader import SourceReader

_uri_start_regex: Final[Pattern] = compile(r"^http(s)?://")
_logger: Final[Logger] = getLogger(__name__)


class HttpsSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return not not match(_uri_start_regex, uri)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
                    elif response.status == 404:
                        raise UriNotFoundException(uri)
                    else:
                        return f"Unable to read '{uri}' ({response.status})"
        except Exception as e:
            raise Exception(f"Unable to read '{uri}'", e)

    # This function interacts with HTTP endpoints inefficiently.
    # See https://github.com/sparkgeo/STAC-API-Serverless/issues/98 for thoughts on this.
    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        # assume this is a STAC API, otherwise no standard way to parse the response
        item_uris: List[str] = []
        errors: List[str] = []
        next_items_uri: str | None = uri
        while next_items_uri is not None:
            items_response: Dict[str, Any] = await self.load_json_from_uri(
                next_items_uri
            )
            if not isinstance(items_response, dict):
                errors.append(f"unexpected response from '{uri}'")
                continue
            for item in items_response.get("features", []):
                for link in cast(Dict[str, Any], item).get("links", []):
                    link = cast(Dict[str, str], link)
                    if link.get("rel", "") == "self":
                        if "href" in link:
                            item_uris.append(link["href"])
                            break
            if item_limit is not None and len(item_uris) == item_limit:
                break
            next_links = [
                link
                for link in items_response.get("links", [])
                if link.get("rel", "") == "next"
                and link.get("method", "GET").upper() == "GET"
            ]
            if len(next_links) == 1:
                if "href" in next_links[0]:
                    next_items_uri = next_links[0]["href"]
                else:
                    next_items_uri = None
            else:
                next_items_uri = None
        return (item_uris, errors)
