from abc import ABC, abstractmethod
from json import loads
from typing import Any, Dict, List, Optional, Tuple


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
    def path_separator(self) -> str:
        pass

    @abstractmethod
    async def get_uri_as_string(self, uri: str) -> str:
        pass

    @abstractmethod
    async def get_uri_to_file(self, uri: str, file_path: str) -> None:
        pass

    @abstractmethod
    async def get_item_uris_from_items_uri(
        self, uri: str, item_limit: Optional[int] = None
    ) -> Tuple[List[str], List[str]]:
        pass

    async def load_json_from_uri(self, uri: str) -> Dict[str, Any]:
        return loads(await self.get_uri_as_string(uri))
