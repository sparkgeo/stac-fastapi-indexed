from logging import (
    DEBUG,
    INFO,
    StreamHandler,
    basicConfig,
    getLevelName,
    getLevelNamesMapping,
    getLogger,
)
from sys import stdout
from typing import Final

from stac_indexer.settings import get_settings

_default_log_level_name: Final[str] = getLevelName(INFO)


requested_log_level = get_settings().log_level.upper()
if requested_log_level not in getLevelNamesMapping():
    print(
        f"Invalid log level '{requested_log_level}', defaulting to '{_default_log_level_name}'"
    )
    requested_log_level = _default_log_level_name
handlers = [
    StreamHandler(stream=stdout),
]
log_level = getLevelNamesMapping()[requested_log_level]
basicConfig(
    handlers=handlers,
    level=log_level,
    format="%(levelname)s %(asctime)s %(message)s",
)
if log_level == DEBUG and not get_settings().permit_boto_debug:
    for logger in [getLogger("botocore"), getLogger("boto3")]:
        logger.setLevel(INFO)
