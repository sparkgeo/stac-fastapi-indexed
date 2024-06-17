from abc import ABC, abstractmethod


class Fetcher(ABC):
    @staticmethod
    @abstractmethod
    def compatibility_regex() -> str:
        pass

    @abstractmethod
    async def fetch(self, url: str) -> str:
        pass
