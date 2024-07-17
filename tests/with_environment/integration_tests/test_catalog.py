from json import load

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    get_collection_file_paths,
    get_link_hrefs_by_rel,
)
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_queryables_enabled():
    queryables_links = get_link_hrefs_by_rel(
        requests.get(api_base_url).json(),
        "http://www.opengis.net/def/rel/ogc/1.0/queryables",
    )
    assert len(queryables_links) == 1


def test_catalog_collections_match():
    catalog_collection_hrefs = get_link_hrefs_by_rel(
        requests.get(api_base_url).json(), "child"
    )
    assert len(catalog_collection_hrefs) > 0
    collection_file_paths = get_collection_file_paths()
    assert len(collection_file_paths) > 0
    for collection_file_path in collection_file_paths:
        with open(collection_file_path) as f:
            collection = load(f)
        assert (
            "{api_base_url}/collections/{collection_id}".format(
                api_base_url=api_base_url,
                collection_id=collection["id"],
            )
            in catalog_collection_hrefs
        )
