from abc import ABC, abstractmethod
from json import loads
from logging import Logger, getLogger
from typing import Any, Dict, Final, List, Optional, Tuple

from stac_index.common.exceptions import UriNotFoundException

_logger: Final[Logger] = getLogger(__name__)


class SourceReader(ABC):
    def __init__(self, *args, **kwargs):
        concurrency = None
        concurrency_key = "concurrency"
        if concurrency_key in kwargs and isinstance(kwargs[concurrency_key], int):
            concurrency = kwargs["concurrency"]
            if concurrency <= 0:
                concurrency = None
        self.reader_concurrency = concurrency

    @staticmethod
    @abstractmethod
    def can_handle_uri(uri: str) -> bool:
        pass

    @abstractmethod
    async def get_uri_as_string(self, uri: str) -> str:
        pass

    @abstractmethod
    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        pass

    async def load_json_from_uri(self, uri: str) -> Dict[str, Any]:
        try:
            json_string = await self.get_uri_as_string(uri)
        except Exception:
            raise UriNotFoundException(uri)
        return loads(json_string)
