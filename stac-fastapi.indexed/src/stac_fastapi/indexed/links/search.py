from typing import Any, Dict
from urllib.parse import urljoin

from fastapi import Request
from stac_fastapi.indexed.constants import rel_next, rel_previous, type_json
from stac_fastapi.indexed.links.util import get_base_href
from stac_fastapi.indexed.search.types import SearchDirection, SearchMethod


def get_search_link(request: Request, rel_type: str) -> Dict[str, Any]:
    return {
        "rel": rel_type,
        "type": type_json,
        "href": urljoin(
            get_base_href(request),
            "/search",
        ),
    }


def get_token_link(
    request: Request,
    search_direction: SearchDirection,
    search_method: SearchMethod,
    token: str,
) -> Dict[str, Any]:
    link_dict_append = {}
    if search_method == SearchMethod.GET:
        search_href = _add_token_to_get_url(request, token)
    elif search_method == SearchMethod.POST:
        search_href = str(request.url)
        link_dict_append = {"body": {"token": token}}
    return {
        **{
            "rel": {
                SearchDirection.Next: rel_next,
                SearchDirection.Previous: rel_previous,
            }[search_direction],
            "type": type_json,
            "href": search_href,
        },
        **link_dict_append,
    }


def _add_token_to_get_url(request: Request, token: str) -> str:
    return str(request.url.replace_query_params(token=token))
