from typing import Final

_uri_start_str: Final[str] = "/"


def can_handle_uri(uri: str) -> bool:
    return uri.startswith(_uri_start_str)


def path_separator() -> str:
    return _uri_start_str  # sorry Windows
