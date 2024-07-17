from json import dumps, load
from typing import Any, Dict

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    get_item_file_paths_for_collection,
    get_link_hrefs_by_rel,
)
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_collection_items_paged():
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
