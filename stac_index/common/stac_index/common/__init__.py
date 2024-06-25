import importlib
import inspect
from logging import Logger, getLogger
from typing import Final, List, Type

from stac_index.common.index_reader import IndexReader
from stac_index.common.source_reader import SourceReader

_logger: Final[Logger] = getLogger(__file__)
_package_prefix: Final[str] = "stac-index.reader."

source_reader_classes: List[Type[SourceReader]] = []
index_reader_classes: List[Type[IndexReader]] = []


for distribution in importlib.metadata.distributions():
    package_name: str = distribution.metadata["Name"]
    if package_name.startswith(_package_prefix):
        _logger.info(f"found stac-index source reader package '{package_name}'")
        # assume that the following creates a valid import path
        importable_name = package_name.replace("-", "_")
        module = importlib.import_module(importable_name)
        for name, object in inspect.getmembers(module):
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


if len(source_reader_classes) == 0:
    raise Exception("no source reader classes discovered, unable to read STAC data")

# base IndexReader is a valid index reader if no more specific readers are available
index_reader_classes.append(IndexReader)
