from abc import ABC, abstractmethod
from typing import Self


class Source(ABC):
    @staticmethod
    @abstractmethod
    def can_handle_uri(uri: str) -> bool:
        pass

    @abstractmethod
    def path_separator(self: Self) -> str:
        pass
