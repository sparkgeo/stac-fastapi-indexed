from logging import Logger, getLogger
from re import Pattern, compile, match
from time import time
from typing import Any, Callable, Coroutine, Dict, Final, List, Optional, Tuple, cast

from aiohttp import ClientResponse, ClientSession
from stac_index.readers.exceptions import UriNotFoundException
from stac_index.readers.source_reader import SourceReader

_uri_start_regex: Final[Pattern] = compile(r"^http(s)?://")
_logger: Final[Logger] = getLogger(__name__)


class HttpsSourceReader(SourceReader):
    @staticmethod
    def can_handle_uri(uri: str) -> bool:
        return not not match(_uri_start_regex, uri)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _logger.info("creating HTTPS Reader")

    def path_separator(self) -> str:
        return "/"

    async def _get_uri_and_process(
        self,
        uri: str,
        processor: Callable[[ClientResponse], Coroutine[None, None, None]],
        success_statuses: List[int] = [200],
    ) -> None:
        start = time()
        async with ClientSession() as session:
            async with session.get(uri) as response:
                if response.status in success_statuses:
                    await processor(response)
                    _logger.debug(
                        "HTTPS: fetched '{}' in {}s".format(
                            uri,
                            round(time() - start, 3),
                        )
                    )
                elif response.status == 404:
                    raise UriNotFoundException(uri)
                else:
                    raise Exception(f"Unable to read '{uri}' ({response.status})")

    async def get_uri_as_string(self, uri: str) -> str:
        result = ""

        async def process(response: ClientResponse) -> None:
            nonlocal result
            result = await response.text()

        await self._get_uri_and_process(uri, process)
        return result

    async def get_uri_to_file(self, uri: str, file_path: str) -> None:
        async def process(response: ClientResponse) -> None:
            with open(file_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1000000):
                    f.write(chunk)

        await self._get_uri_and_process(uri, process)

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
