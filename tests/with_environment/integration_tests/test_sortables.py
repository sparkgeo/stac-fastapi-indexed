from json import load
from typing import Dict, Final, List

import requests
from with_environment.common import api_base_url
from with_environment.integration_tests.common import get_index_config_path
from with_environment.wait import wait_for_api

_global_sortable_titles: Final[List[str]] = [
    "id",
    "collection",
    "datetime",
    "start_datetime",
    "end_datetime",
]


def setup_module():
    wait_for_api()


def test_sortables_all_collections() -> None:
    sortable_property_titles = [
        entry
        for entry in requests.get(f"{api_base_url}/sortables")
        .json()["properties"]
        .keys()
    ]
    assert (
        len(set(_global_sortable_titles).difference(set(sortable_property_titles))) == 0
    )


def test_sortables_by_collection() -> None:
    with open(get_index_config_path(), "r") as f:
        index_config = load(f)
    sortables_by_collection: Dict[str, List[str]] = {}
    for name, queryable in index_config["queryables"].items():
        for collection_id in queryable["collections"]:
            if collection_id not in sortables_by_collection:
                sortables_by_collection[collection_id] = []
            sortables_by_collection[collection_id].append(name)
    for collection_id, sortable_names in sortables_by_collection.items():
        expected_sortables = sortable_names
        collection_sortable_property_titles = [
            name
            for name in requests.get(
                f"{api_base_url}/collections/{collection_id}/sortables"
            )
            .json()["properties"]
            .keys()
        ]
        assert (
            len(
                set(_global_sortable_titles + expected_sortables).difference(
                    set(collection_sortable_property_titles)
                )
            )
            == 0
        )
