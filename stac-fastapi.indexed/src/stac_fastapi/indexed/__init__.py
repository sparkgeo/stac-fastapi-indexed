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

from asgi_correlation_id import CorrelationIdFilter
from stac_fastapi.indexed.settings import get_settings

_default_log_level: Final[int] = INFO
_default_log_level_name: Final[str] = getLevelName(_default_log_level)


def configure_logging() -> None:
    requested_log_level = get_settings().log_level.upper()
    if requested_log_level not in getLevelNamesMapping():
        print(
            f"Invalid log level '{requested_log_level}', defaulting to '{_default_log_level_name}'"
        )
        requested_log_level = _default_log_level_name
    stdout_handler = StreamHandler(stream=stdout)
    stdout_handler.addFilter(CorrelationIdFilter(uuid_length=32).filter)
    handlers = [
        stdout_handler,
    ]
    log_level = getLevelNamesMapping()[requested_log_level]
    basicConfig(
        handlers=handlers,
        level=log_level,
        format="%(levelname)s %(asctime)s [%(correlation_id)s] %(message)s",
        force=True,
    )
    if log_level == DEBUG and not get_settings().permit_boto_debug:
        for logger_name in ["botocore", "boto3", "nose", "s3transfer", "urllib3"]:
            getLogger(logger_name).setLevel(_default_log_level)


configure_logging()
