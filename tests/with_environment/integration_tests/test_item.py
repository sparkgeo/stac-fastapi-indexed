from json import dumps

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import get_link_hrefs_by_rel
from with_environment.wait import wait_for_api


def setup_module():
    wait_for_api()


def test_item():
    collection_hrefs = get_link_hrefs_by_rel(requests.get(api_base_url).json(), "child")
    assert len(collection_hrefs) > 0
    for collection_href in collection_hrefs:
        collection = requests.get(collection_href).json()
        items_links = get_link_hrefs_by_rel(collection, "items")
        for item_from_collection in requests.get(items_links[0]).json()["features"]:
            item_from_item = requests.get(
                "{}/collections/{}/items/{}".format(
                    api_base_url,
                    collection["id"],
                    item_from_collection["id"],
                )
            ).json()
            assert dumps(item_from_collection) == dumps(item_from_item)
