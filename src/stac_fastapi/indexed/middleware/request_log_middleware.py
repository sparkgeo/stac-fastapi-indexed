from logging import Logger, getLogger
from typing import Final

from starlette.types import ASGIApp, Receive, Scope, Send

_logger: Final[Logger] = getLogger(__name__)


class RequestLogMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        _logger.debug("Received request")
        await self.app(scope, receive, send)
        _logger.debug("Created response")
