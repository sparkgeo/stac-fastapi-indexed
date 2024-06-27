from urllib.parse import urljoin

from fastapi import Request
from stac_fastapi.types.stac import Item

from stac_fastapi.indexed.constants import (
    rel_collection,
    rel_parent,
    rel_root,
    rel_self,
    type_geojson,
)
from stac_fastapi.indexed.links.catalog import get_catalog_link
from stac_fastapi.indexed.links.collection import get_collection_link
from stac_fastapi.indexed.links.util import get_base_href


def fix_item_links(item: Item, request: Request) -> Item:
    base_href = get_base_href(request)
    item["links"] = [
        link
        for link in item["links"]
        if link["rel"]
        not in [
            rel_collection,
            rel_parent,
            rel_self,
            rel_root,
        ]
    ] + [
        get_collection_link(request, item["collection"], rel_collection),
        get_collection_link(request, item["collection"], rel_parent),
        get_catalog_link(request, rel_root),
        {
            "rel": rel_self,
            "type": type_geojson,
            "href": urljoin(
                base_href,
                "/collections/{}/items/{}".format(
                    item["collection"],
                    item["id"],
                ),
            ),
        },
    ]
    return item
