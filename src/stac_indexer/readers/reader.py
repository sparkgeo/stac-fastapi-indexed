from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from stac_indexer.stac_data import StacData


class Reader(ABC):
    @classmethod
    @abstractmethod
    def create_reader(cls, url: str) -> Optional["Reader"]:
        pass

    @abstractmethod
    def process(self) -> Tuple[StacData, List[str]]:
        pass
