from json import dumps, load

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    get_collection_file_paths,
    get_link_hrefs_by_rel,
)
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_collection_detail_match():
    collection_hrefs = get_link_hrefs_by_rel(requests.get(api_base_url).json(), "child")
    assert len(collection_hrefs) > 0
    for collection_href in collection_hrefs:
        api_collection = requests.get(collection_href).json()
        found_file_collection = False
        for collection_file_path in get_collection_file_paths():
            with open(collection_file_path) as f:
                file_collection = load(f)
            if file_collection["id"] == api_collection["id"]:
                found_file_collection = True
            else:
                continue
            # exclude links from comparison as they are modified by the API
            assert dumps({**api_collection, "links": []}) == dumps(
                {**file_collection, "links": []}
            )
        assert found_file_collection
