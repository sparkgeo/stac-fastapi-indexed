from typing import Any, Dict

from fastapi import Request

from stac_fastapi_indexed.constants import rel_root, type_json
from stac_fastapi_indexed.links.util import get_base_href


def get_catalog_link(request: Request, rel_type: str = rel_root) -> Dict[str, Any]:
    return {
        "rel": rel_type,
        "type": type_json,
        "href": get_base_href(request),
    }
