from json import dumps, load

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import get_collection_file_paths
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_all_collections_match():
    api_collections = requests.get(f"{api_base_url}/collections").json()["collections"]
    assert len(api_collections) > 0
    for collection_file_path in get_collection_file_paths():
        with open(collection_file_path) as f:
            file_collection = load(f)
        matching_api_collections = [
            coll for coll in api_collections if coll["id"] == file_collection["id"]
        ]
        assert len(matching_api_collections) == 1
        # exclude links from comparison as they are modified by the API
        assert dumps({**matching_api_collections[0], "links": []}) == dumps(
            {**file_collection, "links": []}
        )
