from logging import Logger, getLogger
from typing import Final

from jwt import decode, encode
from stac_fastapi.types.errors import InvalidQueryParameter

from stac_fastapi.indexed.search.query_info import QueryInfo
from stac_fastapi.indexed.settings import get_settings

_hashing_algorithm: Final[str] = "HS256"
_logger: Final[Logger] = getLogger(__name__)


def get_query_from_token(
    token: str,
) -> QueryInfo:
    try:
        return QueryInfo(
            **decode(
                jwt=token,
                key=get_settings().token_jwt_secret,
                algorithms=[_hashing_algorithm],
            )
        ).json_post_decoder()
    except Exception as e:
        _logger.warn("error decoding query token", e)
        raise InvalidQueryParameter("invalid search token")


def create_token_from_query(query_info: QueryInfo) -> str:
    return encode(
        payload=(query_info.to_dict()),
        key=get_settings().token_jwt_secret,
        algorithm=_hashing_algorithm,
        json_encoder=QueryInfo.json_encoder(),
    )
