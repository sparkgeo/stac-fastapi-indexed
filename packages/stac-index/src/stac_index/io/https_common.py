from re import Pattern, compile, match
from typing import Final

_uri_start_regex: Final[Pattern] = compile(r"^http(s)?://")


def can_handle_uri(uri: str) -> bool:
    return not not match(_uri_start_regex, uri)


def path_separator() -> str:
    return "/"
