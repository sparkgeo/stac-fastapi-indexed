from abc import ABC, abstractmethod

from stac_indexer.types.stac_data import StacData


class IndexCreator(ABC):
    @classmethod
    @abstractmethod
    def create_index_creator(cls, stac_data: StacData) -> "IndexCreator":
        pass

    @abstractmethod
    def process(self) -> str:
        pass
