import importlib
import importlib.metadata
import inspect
from logging import Logger, getLogger
from typing import Final, List, Type

from .index_reader import IndexReader
from .source_reader import SourceReader
from .. import readers

_logger: Final[Logger] = getLogger(__file__)
_package_prefix: Final[str] = "stac_index.readers."

source_reader_classes: List[Type[SourceReader]] = []
index_reader_classes: List[Type[IndexReader]] = []


def find_readers(module):
    for _, object in inspect.getmembers(module):
        if (
            inspect.isclass(object)
            and issubclass(object, SourceReader)
            and object is not SourceReader
        ):
            _logger.info(
                f"found stac-index source reader '{object.__name__}' in '{package_name}'"
            )
            source_reader_classes.append(object)
        if (
            inspect.isclass(object)
            and issubclass(object, IndexReader)
            and object is not IndexReader
        ):
            _logger.info(
                f"found stac-index index reader '{object.__name__}' in '{package_name}'"
            )
            index_reader_classes.append(object)


# Search installed packages
for distribution in importlib.metadata.distributions():
    package_name: str = distribution.metadata["Name"]
    if package_name.startswith(_package_prefix):
        _logger.info(f"found stac-index source reader package '{package_name}'")
        # assume that the following creates a valid import path
        importable_name = package_name.replace("-", "_")
        module = importlib.import_module(importable_name)
        find_readers(module)


# Inclde readers from the stac_index package
index_reader_classes.extend(readers.index_reader_classes)
source_reader_classes.extend(readers.source_reader_classes)

if len(source_reader_classes) == 0:
    raise Exception("no source reader classes discovered, unable to read STAC data")

# base IndexReader is a valid index reader if no more specific readers are available
index_reader_classes.append(IndexReader)
