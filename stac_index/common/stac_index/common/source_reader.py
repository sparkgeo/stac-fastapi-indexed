from abc import ABC, abstractmethod
from json import loads
from typing import Any, Dict, List, Optional


class SourceReader(ABC):
    @staticmethod
    @abstractmethod
    def can_handle_uri(uri: str) -> bool:
        pass

    @abstractmethod
    async def get_uri_as_string(self, uri: str) -> str:
        pass

    async def list_uris_by_prefix(
        self,
        uri_prefix: str,
        list_limit: Optional[int] = None,
        uri_suffix: Optional[str] = None,
    ) -> List[str]:
        raise NotImplementedError("Reader does not implement this function")

    async def load_json_from_uri(self, uri: str) -> Dict[str, Any]:
        return loads(await self.get_uri_as_string(uri))
