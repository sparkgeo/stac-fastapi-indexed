from logging import Logger, getLogger
from typing import Final

from fastapi import HTTPException, status
from jwt import decode, encode
from stac_fastapi.types.errors import InvalidQueryParameter

from stac_fastapi.indexed.search.filter_clause import FilterClause
from stac_fastapi.indexed.search.query_info import QueryInfo, current_query_version
from stac_fastapi.indexed.settings import get_settings

_hashing_algorithm: Final[str] = "HS256"
_logger: Final[Logger] = getLogger(__name__)


def get_query_info_from_token(
    token: str,
) -> QueryInfo:
    try:
        token_dict = decode(
            jwt=token,
            key=get_settings().token_jwt_secret,
            algorithms=[_hashing_algorithm],
        )
        result = QueryInfo(
            **{
                **token_dict,
                "ids": FilterClause(**token_dict["ids"])
                if token_dict["ids"] is not None
                else None,
                "collections": FilterClause(**token_dict["collections"])
                if token_dict["collections"] is not None
                else None,
                "bbox": FilterClause(**token_dict["bbox"])
                if token_dict["bbox"] is not None
                else None,
                "intersects": FilterClause(**token_dict["intersects"])
                if token_dict["intersects"] is not None
                else None,
                "datetime": FilterClause(**token_dict["datetime"])
                if token_dict["datetime"] is not None
                else None,
                "filter": FilterClause(**token_dict["filter"])
                if token_dict["filter"] is not None
                else None,
            }
        ).json_post_decoder()
        if result.query_version != current_query_version:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Paging token is no longer compatible with this API. Remove the paging token to start again.",
            )
        return result
    except Exception as e:
        _logger.warning("error decoding query token", e)
        raise InvalidQueryParameter("invalid search token")


def create_token_from_query(query_info: QueryInfo) -> str:
    return encode(
        payload=(query_info.to_dict()),
        key=get_settings().token_jwt_secret,
        algorithm=_hashing_algorithm,
        json_encoder=QueryInfo.json_encoder(),
    )
