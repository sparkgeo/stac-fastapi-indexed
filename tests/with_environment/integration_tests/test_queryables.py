from json import load
from typing import Dict, Final, List

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
    queryables_by_collection: Dict[str, List[str]] = {}
    for name, queryable in index_config["queryables"].items():
        for collection_id in queryable["collections"]:
            if collection_id not in queryables_by_collection:
                queryables_by_collection[collection_id] = []
        queryables_by_collection[collection_id].append(name)
    for collection_id, queryable_names in queryables_by_collection.items():
        collection_queryable_properties = requests.get(
            f"{api_base_url}/collections/{collection_id}/queryables"
        ).json()["properties"]
        assert len(collection_queryable_properties.keys()) == len(
            _global_queryables + queryable_names
        )
        for expected_property_name in _global_queryables + queryable_names:
            assert expected_property_name in collection_queryable_properties
