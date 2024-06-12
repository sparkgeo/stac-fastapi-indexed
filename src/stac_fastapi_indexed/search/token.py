from dataclasses import asdict, dataclass
from logging import Logger, getLogger
from typing import Any, Final, List, Tuple

from jwt import decode, encode
from stac_fastapi.types.errors import InvalidQueryParameter

from stac_fastapi_indexed.settings import get_settings

_hashing_algorithm: Final[str] = "HS256"
_logger: Final[Logger] = getLogger(__file__)


@dataclass
class _TokenizedQuery:
    query: str
    params: List[Any]


def get_query_with_limit_offset_placeholders_from_token(
    token: str,
) -> Tuple[str, List[Any]]:
    try:
        token_content = _TokenizedQuery(
            **decode(
                jwt=token,
                key=get_settings().token_jwt_secret,
                algorithms=[_hashing_algorithm],
            )
        )
        return (token_content.query, token_content.params)
    except Exception as e:
        _logger.warn("error decoding query token", e)
        raise InvalidQueryParameter("invalid search token")


def create_token_from_query_with_limit_offset_placeholders(
    query_with_placeholders: str, params: List[Any]
) -> str:
    return encode(
        payload=asdict(
            _TokenizedQuery(
                query=query_with_placeholders,
                params=params,
            )
        ),
        key=get_settings().token_jwt_secret,
        algorithm=_hashing_algorithm,
    )
