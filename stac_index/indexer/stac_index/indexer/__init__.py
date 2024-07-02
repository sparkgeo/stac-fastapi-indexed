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

from stac_index.indexer.settings import get_settings

_default_log_level: Final[int] = INFO
_default_log_level_name: Final[str] = getLevelName(_default_log_level)

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
    for logger_name in ["botocore", "boto3", "nose", "s3transfer", "urllib3"]:
        getLogger(logger_name).setLevel(_default_log_level)
