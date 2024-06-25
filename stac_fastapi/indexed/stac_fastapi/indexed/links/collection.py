from typing import Any, Dict
from urllib.parse import urljoin

from fastapi import Request
from stac_fastapi.types.stac import Collection

from stac_fastapi.indexed.constants import (
    rel_items,
    rel_parent,
    rel_root,
    rel_self,
    type_geojson,
    type_json,
)
from stac_fastapi.indexed.links.catalog import get_catalog_link
from stac_fastapi.indexed.links.util import get_base_href


def get_collections_link(request: Request, rel_type: str) -> Dict[str, Any]:
    return {
        "rel": rel_type,
        "type": type_json,
        "href": urljoin(
            get_base_href(request),
            "/collections",
        ),
    }


def get_collection_link(
    request: Request, collection_id: str, rel_type: str
) -> Dict[str, Any]:
    return {
        "rel": rel_type,
        "type": type_json,
        "href": urljoin(
            get_base_href(request), "/collections/{}".format(collection_id)
        ),
    }


def fix_collection_links(collection: Collection, request: Request) -> Collection:
    base_href = get_base_href(request)
    collection["links"] = [
        link
        for link in collection["links"]
        if link["rel"]
        not in [
            rel_parent,
            rel_items,
            rel_self,
            rel_root,
        ]
    ] + [
        get_catalog_link(request, rel_root),
        get_catalog_link(request, rel_parent),
        get_collection_link(request, collection["id"], rel_self),
        {
            "rel": rel_items,
            "type": type_geojson,
            "href": urljoin(
                base_href,
                "/collections/{}/items".format(collection["id"]),
            ),
        },
    ]
    return collection
