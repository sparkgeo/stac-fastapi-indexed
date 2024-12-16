from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from stac_pydantic import Item

from stac_index.common.indexing_error import IndexingError, IndexingErrorType


class Fixer(ABC):
    @abstractmethod
    def apply(self, fields: dict[str, Any]) -> dict[str, Any]:
        pass


class StacParserException(Exception):
    def __init__(self, type_: IndexingErrorType, description: str):
        self.indexing_error = IndexingError(
            timestamp=datetime.now(tz=timezone.utc),
            type_=type_,
            description=description,
        )


class StacItemParserException(StacParserException):
    def __init__(self, description: str):
        super().__init__(IndexingErrorType.item_parsing, description)


class StacCollectionParserException(StacParserException):
    def __init__(self, description: str):
        super().__init__(IndexingErrorType.collection_parsing, description)


class StacParser:
    def __init__(self, fixes: list[Fixer]):
        self._fixes = fixes

    def parse_stac_item(self, fields: dict[str, Any]) -> Item:
        for fix in self._fixes:
            fields = fix.apply(fields)
        try:
            return Item(**fields)
        except Exception as e:
            raise StacItemParserException("Error parsing item:'{}'".format(e))
