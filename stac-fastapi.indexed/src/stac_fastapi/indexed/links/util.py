from re import search
from typing import Optional

from fastapi import Request


def get_base_href(request: Request) -> str:
    return "{}://{}".format(
        _get_request_protocol(request),
        _get_header_value_by_name(request, "host"),
    )


# some overlap here with https://github.com/stac-utils/stac-fastapi/blob/main/stac_fastapi/api/stac_fastapi/api/middleware.py#L79
# unfortunately the determined protocol is not persisted by that code, so must be re-determined here
def _get_request_protocol(request: Request) -> str:
    proto = request.scope.get("scheme", "http")
    forwarded = _get_header_value_by_name(request, "forwarded")
    if forwarded is not None:
        parts = forwarded.split(";")
        for part in parts:
            if len(part) > 0 and search("=", part):
                key, value = part.split("=")
                if key == "proto":
                    proto = value
    else:
        proto = _get_header_value_by_name(request, "x-forwarded-proto", proto)
    return proto


def _get_header_value_by_name(
    request: Request, header_name: str, default_value: Optional[str] = None
) -> Optional[str]:
    headers = request.scope["headers"]
    candidates = [
        value.decode() for key, value in headers if key.decode() == header_name
    ]
    return candidates[0] if len(candidates) == 1 else default_value
