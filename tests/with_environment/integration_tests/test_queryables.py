from json import load
from typing import Final, List

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import get_index_config_path
from with_environment.wait import wait_for_api

_global_queryables: Final[List[str]] = [
    "id",
    "collection",
    "geometry",
    "datetime",
    "start_datetime",
    "end_datetime",
]


def setup_module():
    wait_for_api()


def test_queryables_all_collections():
    queryable_properties = requests.get(f"{api_base_url}/queryables").json()[
        "properties"
    ]
    assert len(queryable_properties.keys()) == len(_global_queryables)
    for expected_property_name in _global_queryables:
        assert expected_property_name in queryable_properties


def test_queryables_by_collection():
    with open(get_index_config_path(), "r") as f:
        index_config = load(f)
    for collection_id, queryable_config in index_config["queryables"][
        "collection"
    ].items():
        expected_queryables = list(queryable_config.keys())
        collection_queryable_properties = requests.get(
            f"{api_base_url}/collections/{collection_id}/queryables"
        ).json()["properties"]
        assert len(collection_queryable_properties.keys()) == len(
            _global_queryables + expected_queryables
        )
        for expected_property_name in _global_queryables + expected_queryables:
            assert expected_property_name in collection_queryable_properties
