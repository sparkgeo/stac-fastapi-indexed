from dataclasses import dataclass

from stac_indexer.types.stac_data import StacData


@dataclass
class IndexCreator:
    stac_data: StacData

    def create(self) -> None:
        pass
