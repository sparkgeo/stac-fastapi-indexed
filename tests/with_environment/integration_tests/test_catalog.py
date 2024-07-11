from glob import glob
from json import load
from os import path

import requests
from with_environment.common import api_base_url, stac_json_root_dir
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_catalog_response_collections():
    catalog = requests.get(api_base_url).json()
    catalog_collection_hrefs = [
        link["href"] for link in catalog["links"] if link["rel"] == "child"
    ]
    assert len(catalog_collection_hrefs) > 0
    collection_file_paths = glob(path.join(stac_json_root_dir, "collections", "*.json"))
    assert len(collection_file_paths) > 0
    for (
        collection_file_path
    ) in collection_file_paths:  # intentionally non-recursive to avoid item JSONs
        with open(collection_file_path) as f:
            collection = load(f)
        assert (
            "{api_base_url}/collections/{collection_id}".format(
                api_base_url=api_base_url,
                collection_id=collection["id"],
            )
            in catalog_collection_hrefs
        )
