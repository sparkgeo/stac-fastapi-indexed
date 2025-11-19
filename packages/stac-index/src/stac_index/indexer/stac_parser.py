from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Dict, List, Tuple

from pydantic import ValidationError
from pydantic_core import ErrorDetails
from stac_index.indexer.types.indexing_error import IndexingError, IndexingErrorType

_logger = getLogger(__name__)


class Fixer(ABC):
    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    @abstractmethod
    def check(self, error: ErrorDetails) -> bool:
        """Given a validation error, returns true if the error can be fixed with this Fixer."""
        pass

    @abstractmethod
    def fix(self, fields: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        """Apply the fix to a dictionary.

        Given a dictionary that should represent am Item, return a new
        dictionary with the fix applied.

        If the fix does not apply to the input dict, return the dict
        unchanged along with a False outcome.

        If the fix is applied, return the changed dict along with a
        True outcome.

        """
        pass


class EOExtensionUriFixer(Fixer):
    """Fixes the common problem where an extension is given by name instead or URI.

    Currently only works with the Earth Observation extension, replacing "eo"
    with "https://stac-extensions.github.io/eo/v1.0.0/schema.json".

    """

    @staticmethod
    def name() -> str:
        return "eo-extension-uri"

    def check(self, error: ErrorDetails) -> bool:
        type_key = "type"
        location_key = "loc"
        if type_key in error and location_key in error:
            return (
                error[type_key] == "url_parsing"
                and error[location_key][0] == "stac_extensions"
            )
        else:
            raise Exception("some expected keys missing from error")

    def fix(self, fields: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        result = deepcopy(fields)
        fix_applied = False
        for i, elem in enumerate(result.get("stac_extensions", [])):
            if str(elem).lower() == "eo":
                result["stac_extensions"][i] = (
                    "https://stac-extensions.github.io/eo/v1.0.0/schema.json"
                )
                fix_applied = True
        return (result, fix_applied)


class StacParserException(Exception):
    """Exception class that wraps an IndexingError."""

    def __init__(self, indexing_errors: List[IndexingError]):
        self.indexing_errors = indexing_errors


class StacParser:
    """Constructs STAC Items from a dictionary of fields, optionally applying fixes.

    Stores a list of all fixers and a list of active filters. Active filters are
    specified in __init__() and are applied to all item dictionaries before
    conversion. If constructing a Item fails, the raised error will list and
    fixers (taken from the _all_fixers list) that could have possibly been used
    to repair the Item.

    """

    def __init__(self, fixers: List[str]):
        self._all_fixers: list[Fixer] = [EOExtensionUriFixer()]
        self._active_fixers: list[Fixer] = []
        for fixer_name in fixers:
            for fixer in self._all_fixers:
                if fixer.name() == fixer_name:
                    self._active_fixers.append(fixer)
                    _logger.info("Enabling fixer: {}".format(fixer_name))
                    break

    def parse_stac_item(
        self, fields: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], set[str]]:
        applied_fix_names: set[str] = set()
        for fixer in self._active_fixers:
            fields, changed = fixer.fix(fields)
            if changed is True:
                applied_fix_names.add(fixer.name())
        try:
            return (fields, applied_fix_names)
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
                        collection=fields.get("collection"),
                        item=fields.get("id"),
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
                        description=str(e),
                        collection=fields.get("collection"),
                        item=fields.get("id"),
                    )
                ]
            )
