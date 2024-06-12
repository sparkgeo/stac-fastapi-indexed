from typing import Any, Dict
from urllib.parse import urljoin

from fastapi import Request

from stac_fastapi_indexed.constants import rel_next, rel_previous, type_json
from stac_fastapi_indexed.links.util import get_base_href
from stac_fastapi_indexed.search.types import SearchDirection, SearchMethod


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
    common_search_href = urljoin(get_base_href(request), "/search{link_append}")
    link_dict_append = {}
    if search_method == SearchMethod.GET:
        search_href = common_search_href.format(link_append=f"?token={token}")
    elif search_method == SearchMethod.POST:
        search_href = common_search_href.format(link_append="")
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
