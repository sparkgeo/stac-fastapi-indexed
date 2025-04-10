from json import dumps, load
from re import match
from typing import Any, Dict

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    get_claims_from_token,
    get_item_file_paths_for_collection,
    get_link_dict_by_rel,
    get_link_hrefs_by_rel,
    rebuild_token_with_altered_claims,
)
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_collection_items_paged() -> None:
    collection_hrefs = get_link_hrefs_by_rel(requests.get(api_base_url).json(), "child")
    assert len(collection_hrefs) > 0
    for collection_href in collection_hrefs:
        collection = requests.get(collection_href).json()
        items_from_api: Dict[str, Dict[str, Any]] = {}
        items_from_file: Dict[str, Dict[str, Any]] = {}
        for item_file_path in get_item_file_paths_for_collection(collection["id"]):
            with open(item_file_path, "r") as f:
                item_from_file = load(f)
            items_from_file[item_from_file["id"]] = item_from_file
        items_links = get_link_hrefs_by_rel(collection, "items")
        assert len(items_links) == 1
        next_items_link = items_links[0]
        last_items_link = None
        while next_items_link is not None:
            items_response = requests.get(next_items_link).json()
            for item_from_api in items_response["features"]:
                items_from_api[item_from_api["id"]] = item_from_api
            next_links = get_link_hrefs_by_rel(items_response, "next")
            if len(next_links) == 1:
                next_items_link = next_links[0]
                if last_items_link is not None and next_items_link == last_items_link:
                    raise Exception("next items link is not advancing")
                last_items_link = next_items_link
            else:
                next_items_link = None
        assert len(items_from_api.keys()) == len(items_from_file.keys())
        for id, item_from_api in items_from_api.items():
            # exclude links from comparison as they are modified by the API
            assert dumps({**item_from_api, "links": []}) == dumps(
                {**items_from_file[id], "links": []}
            )


def test_collection_items_token_immutable() -> None:
    collection_hrefs = get_link_hrefs_by_rel(requests.get(api_base_url).json(), "child")
    assert len(collection_hrefs) > 0
    limit = 1
    collection = requests.get(collection_hrefs[0], params={"limit": limit}).json()
    items_links = get_link_hrefs_by_rel(collection, "items")
    assert len(items_links) == 1
    items_link = items_links[0]
    items = requests.get(items_link, params={"limit": limit}).json()
    next_link = get_link_dict_by_rel(items, "next")[0]
    token_match = match(r".+\?token=(.+)$", next_link["href"])
    assert token_match
    token = token_match.group(1)
    try:
        token_claims = get_claims_from_token(token)
    except Exception as e:
        raise Exception(f"token decode failed on '{token}', link '{next_link}'", e)
    assert "limit" in token_claims
    assert token_claims["limit"] == limit
    altered_claims = {
        **token_claims,
        "limit": limit + 1,
    }
    altered_token = rebuild_token_with_altered_claims(token, altered_claims)
    response = requests.get(items_link, params={"token": altered_token})
    assert response.status_code == 400
