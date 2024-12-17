from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, List

from pydantic import ValidationError
from stac_pydantic import Item

from stac_index.common.indexing_error import IndexingError, IndexingErrorType


class Fixer(ABC):
    @abstractmethod
    def apply(self, fields: dict[str, Any]) -> dict[str, Any]:
        pass


class StacParserException(Exception):
    def __init__(self, indexing_errors: List[IndexingError]):
        self.indexing_errors = indexing_errors


class StacParser:
    def __init__(self, fixes: list[Fixer]):
        self._fixes = fixes

    def parse_stac_item(self, fields: dict[str, Any]) -> Item:
        for fix in self._fixes:
            fields = fix.apply(fields)
        try:
            return Item(**fields)
        except ValidationError as e:
            raise StacParserException(
                [
                    IndexingError(
                        timestamp=datetime.now(tz=timezone.utc),
                        type=IndexingErrorType.item_parsing,
                        subtype=error["type"],
                        input_location=", ".join(map(str, error["loc"])),
                        description=error["msg"],
                    )
                    for error in e.errors()
                ]
            )
        except Exception as e:
            raise StacParserException(
                [
                    IndexingError(
                        timestamp=datetime.now(tz=timezone.utc),
                        type=IndexingErrorType.item_parsing,
                        subtype="",
                        input_location="",
                        description=str(e),
                    )
                ]
            )
