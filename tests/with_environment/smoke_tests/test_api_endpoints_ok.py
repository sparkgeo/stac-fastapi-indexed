from typing import Any, Dict

import requests
from with_environment.common import api_base_url
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_get_catalog_endpoint():
    assert requests.get(api_base_url).ok


def test_get_conformance_endpoint():
    assert requests.get(f"{api_base_url}/conformance").ok


def test_get_collections_endpoint():
    assert requests.get(f"{api_base_url}/collections").ok


def test_get_collection_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{api_base_url}/collections/{collection_id}").ok


def test_get_collection_items_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{api_base_url}/collections/{collection_id}/items").ok


def test_get_collection_item_endpoint():
    collection_id = _get_single_collection()["id"]
    item_id = requests.get(f"{api_base_url}/collections/{collection_id}/items").json()[
        "features"
    ][0]["id"]
    assert requests.get(
        f"{api_base_url}/collections/{collection_id}/items/{item_id}"
    ).ok


def test_get_collection_queryables_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{api_base_url}/collections/{collection_id}/queryables").ok


def test_get_queryables_endpoint():
    assert requests.get(f"{api_base_url}/queryables").ok


def test_get_search_empty_endpoint():
    assert requests.get(f"{api_base_url}/search").ok


def test_post_search_empty_endpoint():
    assert requests.post(f"{api_base_url}/search", json={}).ok


def _get_single_collection() -> Dict[str, Any]:
    return requests.get(f"{api_base_url}/collections").json()["collections"][0]
