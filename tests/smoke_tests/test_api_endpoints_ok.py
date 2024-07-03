from datetime import datetime, timezone
from os import environ
from time import sleep
from typing import Any, Dict, Final

import requests

_api_base_url: Final[str] = environ["API_ROOT_PATH"]
_healthcheck_url: Final[str] = f"{_api_base_url}_mgmt/ping"
_healthcheck_timeout_seconds: Final[int] = int(
    environ.get("API_HEALTHCHECK_TIMEOUT_SECONDS", 120)
)
_healthcheck_check_interval_seconds: Final[int] = 1


def setup_module():
    timer = datetime.now(tz=timezone.utc)
    while (
        datetime.now(tz=timezone.utc) - timer
    ).seconds < _healthcheck_timeout_seconds:
        try:
            requests.get(_healthcheck_url)
            print("API available, executing tests")
            return
        except Exception:
            print("waiting for API to become available")
            sleep(_healthcheck_check_interval_seconds)
    raise Exception(
        f"API unavailable after {_healthcheck_timeout_seconds} seconds, failing tests"
    )


def test_get_catalog_endpoint():
    assert requests.get(_api_base_url).ok


def test_get_conformance_endpoint():
    assert requests.get(f"{_api_base_url}conformance").ok


def test_get_collections_endpoint():
    assert requests.get(f"{_api_base_url}collections").ok


def test_get_collection_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{_api_base_url}collections/{collection_id}").ok


def test_get_collection_items_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{_api_base_url}collections/{collection_id}/items").ok


def test_get_collection_item_endpoint():
    collection_id = _get_single_collection()["id"]
    item_id = requests.get(f"{_api_base_url}collections/{collection_id}/items").json()[
        "features"
    ][0]["id"]
    assert requests.get(
        f"{_api_base_url}collections/{collection_id}/items/{item_id}"
    ).ok


def test_get_collection_queryables_endpoint():
    collection_id = _get_single_collection()["id"]
    assert requests.get(f"{_api_base_url}collections/{collection_id}/queryables").ok


def test_get_queryables_endpoint():
    assert requests.get(f"{_api_base_url}queryables").ok


def test_get_search_empty_endpoint():
    assert requests.get(f"{_api_base_url}search").ok


def test_post_search_empty_endpoint():
    assert requests.post(f"{_api_base_url}search", json={}).ok


def _get_single_collection() -> Dict[str, Any]:
    return requests.get(f"{_api_base_url}collections").json()["collections"][0]
