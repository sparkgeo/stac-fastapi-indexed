from abc import abstractmethod
from typing import Self

from stac_index.io.source import Source


class SourceWriter(Source):

    @abstractmethod
    async def put_file_to_uri(self: Self, file_path: str, uri: str) -> None:
        pass
