from logging import INFO, StreamHandler, basicConfig, getLevelName, getLevelNamesMapping
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
        format="%(levelname)s %(asctime)s [%(correlation_id)s] %(name)s: %(message)s",
        force=True,
    )


configure_logging()
