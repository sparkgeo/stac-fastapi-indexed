from datetime import datetime, timedelta
from json import dumps
from typing import Any, Dict, List

import requests
from shapely.geometry import Polygon, box, mapping
from shapely.ops import unary_union
from with_environment.common import api_base_url
from with_environment.integration_tests.common import (
    all_post_search_results,
    compare_results_to_expected,
    get_claims_from_token,
    get_items_with_intersecting_datetime,
    get_link_dict_by_rel,
    get_test_items,
    rebuild_token_with_altered_claims,
)
from with_environment.wait import wait_for_api

_all_collections: List[Dict[str, Any]] = []
_all_items_by_collection_id: Dict[str, List[Dict[str, Any]]] = {}
_all_items: List[Dict[str, Any]] = []


def setup_module():
    wait_for_api()
    global _all_collections
    global _all_items_by_collection_id
    global _all_items
    _all_collections, _all_items_by_collection_id, _all_items = get_test_items()


def test_post_search_blank() -> None:
    compare_results_to_expected(_all_items, all_post_search_results({}))


def test_post_search_collection() -> None:
    collection_id = list(_all_items_by_collection_id.keys())[0]
    compare_results_to_expected(
        _all_items_by_collection_id[collection_id],
        all_post_search_results({"collections": [collection_id]}),
    )


def test_post_search_ids() -> None:
    # assumes more than 3 items in test dataset
    test_items = _all_items[:3]
    compare_results_to_expected(
        test_items,
        all_post_search_results({"ids": [item["id"] for item in test_items]}),
    )


def test_post_search_bbox() -> None:
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
    search_results = all_post_search_results(
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


def test_post_search_intersects() -> None:
    test_collection = _all_collections[0]
    test_items = _all_items_by_collection_id[test_collection["id"]][:2]
    test_polygon: Polygon = None
    for test_item in test_items:
        if test_polygon is None:
            test_polygon = Polygon(test_item["geometry"]["coordinates"][0])
        else:
            test_polygon = unary_union(
                [test_polygon, Polygon(test_item["geometry"]["coordinates"][0])]
            )
    search_results = all_post_search_results(
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


def test_post_search_datetime_include() -> None:
    unique_datetimes = set(
        [
            item["properties"]["datetime"] or item["properties"]["start_datetime"]
            for item in _all_items
        ]
    )
    assert len(unique_datetimes) > 0
    test_datetime = list(unique_datetimes)[0]
    expected_items = get_items_with_intersecting_datetime(_all_items, test_datetime)
    compare_results_to_expected(
        expected_items, all_post_search_results({"datetime": test_datetime})
    )


def test_post_search_datetime_exclude() -> None:
    unique_datetimes = set(
        [
            item["properties"]["datetime"] or item["properties"]["start_datetime"]
            for item in _all_items
        ]
    )
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[0]) + timedelta(days=-1)
    ).isoformat()
    assert test_datetime not in unique_datetimes
    assert len(all_post_search_results({"datetime": test_datetime})) == 0


def test_post_search_datetime_open_start() -> None:
    unique_datetimes = set(
        [
            item["properties"]["datetime"] or item["properties"]["end_datetime"]
            for item in _all_items
        ]
    )
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[-1]) + timedelta(days=1)
    ).isoformat()
    compare_results_to_expected(
        _all_items, all_post_search_results({"datetime": f"../{test_datetime}"})
    )


def test_post_search_datetime_open_end() -> None:
    unique_datetimes = set(
        [
            item["properties"]["datetime"] or item["properties"]["start_datetime"]
            for item in _all_items
        ]
    )
    assert len(unique_datetimes) > 0
    test_datetime = (
        datetime.fromisoformat(sorted(list(unique_datetimes))[0]) + timedelta(days=-1)
    ).isoformat()
    compare_results_to_expected(
        _all_items, all_post_search_results({"datetime": f"{test_datetime}/.."})
    )


def test_post_search_limit() -> None:
    limit = 1
    assert len(_all_items) > limit
    search_result = requests.post(
        f"{api_base_url}/search", json={"limit": limit}
    ).json()
    assert len(search_result["features"]) == limit
    assert len(get_link_dict_by_rel(search_result, "next")) == 1


def test_post_search_token() -> None:
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


def test_post_search_token_immutable() -> None:
    limit = 1
    assert len(_all_items) > limit
    search_result = requests.post(
        f"{api_base_url}/search", json={"limit": limit}
    ).json()
    next_link = get_link_dict_by_rel(search_result, "next")[0]
    token = next_link["body"]["token"]
    token_claims = get_claims_from_token(token)
    assert "limit" in token_claims
    assert token_claims["limit"] == limit
    altered_claims = {
        **token_claims,
        "limit": limit + 1,
    }
    altered_token = rebuild_token_with_altered_claims(token, altered_claims)
    response = requests.post(f"{api_base_url}/search", json={"token": altered_token})
    assert response.status_code == 400


def test_post_search_alternate_order() -> None:
    sortable_field_names: List[str] = [
        entry["title"]
        for entry in requests.get(f"{api_base_url}/sortables").json()["fields"]
    ]
    assert "datetime" in sortable_field_names
    assert "id" in sortable_field_names

    def sort_items(item: Dict[str, Any]) -> str:
        item_datetime = item["properties"]["datetime"]
        if item_datetime is None:
            item_datetime = datetime.fromtimestamp(0)
        return "{}_{}".format(item_datetime, item["id"])

    expected_items_sorted = _all_items[:]
    expected_items_sorted.sort(
        key=sort_items,
    )
    expected_items_sorted.reverse()  # give descending order to match query
    assert len(expected_items_sorted) == len(_all_items)
    assert expected_items_sorted != _all_items
    sorted_search_results = all_post_search_results(
        {
            "sortby": [
                {"field": "datetime", "direction": "desc"},
                {"field": "id", "direction": "desc"},
            ]
        }
    )
    assert [item["id"] for item in sorted_search_results] == [
        item["id"] for item in expected_items_sorted
    ]
