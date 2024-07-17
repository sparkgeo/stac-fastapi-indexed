from datetime import datetime, timedelta
from json import dumps, load
from typing import Any, Dict, List, Optional

import requests
from shapely.geometry import Polygon, box, mapping
from shapely.ops import unary_union
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    compare_results_to_expected,
    get_collection_file_paths,
    get_item_file_paths_for_collection,
    get_link_dict_by_rel,
)
from with_environment.wait import wait_for_api

_all_collections: List[Dict[str, Any]] = []
_all_items_by_collection_id: Dict[str, List[Dict[str, Any]]] = {}
_all_items: List[Dict[str, Any]] = []


def setup_module():
    wait_for_api()
    for collection_file_path in get_collection_file_paths():
        with open(collection_file_path, "r") as f:
            collection = load(f)
            _all_collections.append(collection)
        _all_items_by_collection_id[collection["id"]] = []
        for item_file_path in get_item_file_paths_for_collection(collection["id"]):
            with open(item_file_path, "r") as f:
                _all_items_by_collection_id[collection["id"]].append(load(f))
    _all_items.extend(
        [item for sublist in _all_items_by_collection_id.values() for item in sublist]
    )


def test_post_search_blank():
    compare_results_to_expected(_all_items, _all_post_search_results({}))


def test_post_search_collection():
    collection_id = list(_all_items_by_collection_id.keys())[0]
    compare_results_to_expected(
        _all_items_by_collection_id[collection_id],
        _all_post_search_results({"collections": [collection_id]}),
    )


def test_post_search_ids():
    # assumes more than 3 items in test dataset
    test_items = _all_items[:3]
    compare_results_to_expected(
        test_items,
        _all_post_search_results({"ids": [item["id"] for item in test_items]}),
    )


def test_post_search_bbox():
    test_collection = _all_collections[0]
    test_items = _all_items[:2]
    test_polygon: Polygon = None
    for test_item in test_items:
        if test_polygon is None:
            test_polygon = Polygon(test_item["geometry"]["coordinates"][0])
        else:
            test_polygon = unary_union(
                [test_polygon, Polygon(test_item["geometry"]["coordinates"][0])]
            )
    test_bbox = test_polygon.bounds
    search_results = _all_post_search_results(
        {"collections": [test_collection["id"]], "bbox": test_bbox}
    )
    assert len(search_results) > 0
    assert len(search_results) < len(_all_items)
    expected_items = [
        item
        for item in _all_items_by_collection_id[test_collection["id"]]
        if box(*Polygon(item["geometry"]["coordinates"][0]).bounds).intersects(
            box(*test_bbox)
        )
    ]
    compare_results_to_expected(expected_items, search_results)


def test_post_search_intersects():
    test_collection = _all_collections[0]
    test_items = _all_items[:2]
    test_polygon: Polygon = None
    for test_item in test_items:
        if test_polygon is None:
            test_polygon = Polygon(test_item["geometry"]["coordinates"][0])
        else:
            test_polygon = unary_union(
                [test_polygon, Polygon(test_item["geometry"]["coordinates"][0])]
            )
    search_results = _all_post_search_results(
        {"collections": [test_collection["id"]], "intersects": mapping(test_polygon)}
    )
    assert len(search_results) > 0
    assert len(search_results) < len(_all_items)
    expected_items = [
        item
        for item in _all_items_by_collection_id[test_collection["id"]]
        if Polygon(item["geometry"]["coordinates"][0]).intersects(test_polygon)
    ]
    compare_results_to_expected(expected_items, search_results)


def test_post_search_datetime_include():
    unique_datetimes = set([item["properties"]["datetime"] for item in _all_items])
    assert len(unique_datetimes) > 0
    test_datetime = list(unique_datetimes)[0]
    expected_items = [
        item for item in _all_items if item["properties"]["datetime"] == test_datetime
    ]
    compare_results_to_expected(
        expected_items, _all_post_search_results({"datetime": test_datetime})
    )


def test_post_search_datetime_exclude():
    unique_datetimes = set([item["properties"]["datetime"] for item in _all_items])
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[0]) + timedelta(days=-1)
    ).isoformat()
    assert test_datetime not in unique_datetimes
    assert len(_all_post_search_results({"datetime": test_datetime})) == 0


def test_post_search_datetime_open_start():
    unique_datetimes = set([item["properties"]["datetime"] for item in _all_items])
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[0]) + timedelta(days=1)
    ).isoformat()
    compare_results_to_expected(
        _all_items, _all_post_search_results({"datetime": f"../{test_datetime}"})
    )


def test_post_search_datetime_open_end():
    unique_datetimes = set([item["properties"]["datetime"] for item in _all_items])
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[0]) + timedelta(days=-1)
    ).isoformat()
    compare_results_to_expected(
        _all_items, _all_post_search_results({"datetime": f"{test_datetime}/.."})
    )


def test_post_search_limit():
    limit = 1
    assert len(_all_items) > limit
    search_result = requests.post(
        f"{api_base_url}/search", json={"limit": limit}
    ).json()
    assert len(search_result["features"]) == limit
    assert len(get_link_dict_by_rel(search_result, "next")) == 1


def test_post_search_token():
    limit = 1
    assert len(_all_items) > limit
    search_result = requests.post(
        f"{api_base_url}/search", json={"limit": limit}
    ).json()
    first_item = search_result["features"][0]
    next_link = get_link_dict_by_rel(search_result, "next")[0]
    next_result = requests.post(f"{api_base_url}/search", json=next_link["body"]).json()
    previous_link = get_link_dict_by_rel(next_result, "previous")[0]
    previous_result = requests.post(
        f"{api_base_url}/search", json=previous_link["body"]
    ).json()
    previous_item = previous_result["features"][0]
    assert dumps(previous_item) == dumps(first_item)


def _all_post_search_results(post_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    last_search_data = None
    search_data: Optional[Dict[str, Any]] = post_data.copy()
    while search_data is not None:
        response = requests.post(f"{api_base_url}/search", json=search_data).json()
        all_results.extend(response["features"])
        next_links = get_link_dict_by_rel(response, "next")
        if len(next_links) == 1:
            last_search_data = search_data
            search_data = next_links[0]["body"]
            if dumps(last_search_data) == dumps(search_data):
                raise Exception("next search link is not advancing")
        else:
            search_data = None
    return all_results
