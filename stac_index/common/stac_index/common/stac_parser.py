import re
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timezone
from logging import getLogger
from typing import Any, List, Tuple

from pydantic import ValidationError
from pydantic_core import ErrorDetails
from stac_pydantic import Item

from stac_index.common.indexing_error import IndexingError, IndexingErrorType

_logger = getLogger(__file__)


class Fixer(ABC):
    def name(self) -> str:
        # Get class name and convert it to snake case
        return (
            re.sub(r"(?<!^)(?=[A-Z])", "-", type(self).__name__)
            .lower()
            .removesuffix("-fixer")
        )

    @abstractmethod
    def check(self, error: ErrorDetails) -> bool:
        """Given a validation error, returns true if the error can be fixed with this Fixer."""
        pass

    @abstractmethod
    def fix(self, fields: dict[str, Any]) -> dict[str, Any]:
        pass


class ExtensionUriFixer(Fixer):
    def check(self, error: ErrorDetails) -> bool:
        return error["type"] == "url_parsing" and error["loc"][0] == "stac_extensions"

    def fix(self, fields: dict[str, Any]) -> dict[str, Any]:
        result = deepcopy(fields)
        for i, elem in enumerate(result.get("stac_extensions", [])):
            if elem == "eo":
                result["stac_extensions"][i] = (
                    "https://stac-extensions.github.io/eo/v1.0.0/schema.json"
                )
                applied_fixes = result.get("applied_fixes", set())
                applied_fixes.add(self.name())
                result["applied_fixes"] = applied_fixes
        return result


class StacParserException(Exception):
    def __init__(self, indexing_errors: List[IndexingError]):
        self.indexing_errors = indexing_errors


class StacParser:
    def __init__(self, fixers: list[str]):
        self._all_fixers = [ExtensionUriFixer()]
        self._active_fixers = []
        for fixer_name in fixers:
            for fixer in self._all_fixers:
                if fixer.name() == fixer_name:
                    self._active_fixers.append(fixer)
                    _logger.info("Enabling fixer: {}".format(fixer_name))
                    break

    def parse_stac_item(self, fields: dict[str, Any]) -> Tuple[Item, dict[str, Any]]:
        for fixer in self._active_fixers:
            fields = fixer.fix(fields)
        try:
            return (Item(**fields), fields)
        except ValidationError as e:
            raise StacParserException(
                [
                    IndexingError(
                        timestamp=datetime.now(tz=timezone.utc),
                        type=IndexingErrorType.item_parsing,
                        subtype=error["type"],
                        input_location=", ".join(map(str, error["loc"])),
                        description=error["msg"],
                        possible_fixes=", ".join(
                            [
                                fixer.name()
                                for fixer in self._all_fixers
                                if fixer.check(error)
                            ]
                        ),
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
                        possible_fixes="",
                    )
                ]
            )
