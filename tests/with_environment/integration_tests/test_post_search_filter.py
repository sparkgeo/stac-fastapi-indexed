from datetime import datetime
from typing import Any, Dict, List, cast

from shapely.geometry import Polygon, mapping
from with_environment.integration_tests.common import (
    all_post_search_results,
    compare_results_to_expected,
    get_test_items,
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


def test_post_search_filter_numeric_between_include() -> None:
    unique_gsd = sorted(list(set([item["properties"]["gsd"] for item in _all_items])))
    gsd_min = unique_gsd[0] - 0.1
    gsd_max = unique_gsd[-1] + 0.1
    expected_items = [
        item
        for item in _all_items
        if item["properties"]["gsd"] > gsd_min and item["properties"]["gsd"] < gsd_max
    ]
    assert len(expected_items) > 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "between",
                    "args": [
                        {
                            "property": "gsd",
                        },
                        [gsd_min, gsd_max],
                    ],
                },
            }
        ),
    )


def test_post_search_filter_numeric_between_exclude() -> None:
    unique_gsd = sorted(list(set([item["properties"]["gsd"] for item in _all_items])))
    assert len(unique_gsd) > 1
    gsd_min = unique_gsd[-1] + 0.1
    gsd_max = gsd_min + 0.1
    expected_items = [
        item
        for item in _all_items
        if item["properties"]["gsd"] > gsd_min and item["properties"]["gsd"] < gsd_max
    ]
    assert len(expected_items) == 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "between",
                    "args": [
                        {
                            "property": "gsd",
                        },
                        [gsd_min, gsd_max],
                    ],
                },
            }
        ),
    )


def test_post_search_filter_numeric_gt() -> None:
    unique_gsd = sorted(list(set([item["properties"]["gsd"] for item in _all_items])))
    assert len(unique_gsd) > 2
    gsd_range = unique_gsd[-1] - unique_gsd[0]
    assert gsd_range > 0
    gsd_splitter = unique_gsd[0] + gsd_range / 2
    expected_items = [
        item for item in _all_items if item["properties"]["gsd"] > gsd_splitter
    ]
    assert len(expected_items) > 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "gt",
                    "args": [
                        {
                            "property": "gsd",
                        },
                        gsd_splitter,
                    ],
                },
            }
        ),
    )


def test_post_search_filter_numeric_lt() -> None:
    unique_gsd = sorted(list(set([item["properties"]["gsd"] for item in _all_items])))
    assert len(unique_gsd) > 2
    gsd_range = unique_gsd[-1] - unique_gsd[0]
    assert gsd_range > 0
    gsd_splitter = unique_gsd[0] + gsd_range / 2
    expected_items = [
        item for item in _all_items if item["properties"]["gsd"] < gsd_splitter
    ]
    assert len(expected_items) > 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "lt",
                    "args": [
                        {
                            "property": "gsd",
                        },
                        gsd_splitter,
                    ],
                },
            }
        ),
    )


def test_post_search_filter_string_equals() -> None:
    test_item = _all_items[0]
    compare_results_to_expected(
        [test_item],
        all_post_search_results(
            {
                "filter": {
                    "op": "and",
                    "args": [
                        {
                            "op": "=",
                            "args": [
                                {"property": "collection"},
                                test_item["collection"],
                            ],
                        },
                        {"op": "=", "args": [{"property": "id"}, test_item["id"]]},
                    ],
                },
            }
        ),
    )


def test_post_search_filter_not() -> None:
    exclude_item = _all_items[0]
    expected_items = [
        item
        for item in _all_items
        if not (
            item["id"] == exclude_item["id"]
            and item["collection"] == exclude_item["collection"]
        )
    ]
    assert len(expected_items) > 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "not",
                    "args": [
                        {
                            "op": "and",
                            "args": [
                                {
                                    "op": "=",
                                    "args": [
                                        {"property": "collection"},
                                        exclude_item["collection"],
                                    ],
                                },
                                {
                                    "op": "=",
                                    "args": [{"property": "id"}, exclude_item["id"]],
                                },
                            ],
                        }
                    ],
                },
            }
        ),
    )


def test_post_search_filter_or() -> None:
    or_list_length = 3
    assert len(_all_items) > or_list_length
    or_list = _all_items[:or_list_length]
    compare_results_to_expected(
        or_list,
        all_post_search_results(
            {
                "filter": {
                    "op": "or",
                    "args": [
                        {
                            "op": "and",
                            "args": [
                                {
                                    "op": "=",
                                    "args": [
                                        {"property": "collection"},
                                        or_item["collection"],
                                    ],
                                },
                                {
                                    "op": "=",
                                    "args": [{"property": "id"}, or_item["id"]],
                                },
                            ],
                        }
                        for or_item in or_list
                    ],
                },
            }
        ),
    )


def test_post_search_filter_string_like() -> None:
    partial_id_length = 4
    first_item = _all_items[0]
    partial_id = first_item["id"][:partial_id_length]
    assert len(first_item["id"]) > partial_id_length
    expected_items = [
        item
        for item in _all_items
        if item["collection"] == first_item["collection"]
        and cast(str, item["id"]).startswith(partial_id)
    ]
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "and",
                    "args": [
                        {
                            "op": "=",
                            "args": [
                                {"property": "collection"},
                                first_item["collection"],
                            ],
                        },
                        {"op": "like", "args": [{"property": "id"}, f"{partial_id}%"]},
                    ],
                },
            }
        ),
    )


def test_post_search_filter_string_in() -> None:
    in_list_length = 3
    assert len(_all_items) >= in_list_length
    in_list_items = _all_items[:in_list_length]
    compare_results_to_expected(
        in_list_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "in",
                    "args": [
                        {
                            "property": "id",
                        },
                        [item["id"] for item in in_list_items]
                        + ["non-existent-id-that-should-not-match"],
                    ],
                },
            }
        ),
    )


def test_post_search_filter_point_intersect() -> None:
    test_collection = _all_collections[0]
    test_item = _all_items_by_collection_id[test_collection["id"]][0]
    test_point = Polygon(test_item["geometry"]["coordinates"][0]).centroid
    search_results = all_post_search_results(
        {
            "collections": [test_collection["id"]],
            "filter": {
                "op": "s_intersects",
                "args": [
                    {"property": "geometry"},
                    mapping(test_point),
                ],
            },
        }
    )
    assert len(search_results) > 0
    expected_items = [
        item
        for item in _all_items_by_collection_id[test_collection["id"]]
        if Polygon(item["geometry"]["coordinates"][0]).intersects(test_point)
    ]
    compare_results_to_expected(expected_items, search_results)


def test_post_search_filter_temporal_point_intersect() -> None:
    unique_point_datetimes = set(
        [
            item
            for item in [item["properties"]["datetime"] for item in _all_items]
            if item is not None
        ]
    )
    assert len(unique_point_datetimes) > 2
    test_interval_start = "1970-01-01T00:00:00Z"
    test_interval_end = list(unique_point_datetimes)[1]
    expected_items: List[Dict[str, Any]] = []
    for item in _all_items:
        properties = item["properties"]
        if properties["datetime"] is not None:
            if datetime.fromisoformat(properties["datetime"]) >= datetime.fromisoformat(
                test_interval_start
            ) and datetime.fromisoformat(
                properties["datetime"]
            ) <= datetime.fromisoformat(test_interval_end):
                expected_items.append(item)
    assert len(expected_items) > 0
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "t_intersects",
                    "args": [
                        {"property": "datetime"},
                        {
                            "interval": [
                                test_interval_start,
                                test_interval_end,
                            ]
                        },
                    ],
                },
            }
        ),
    )


def test_post_search_filter_temporal_range_intersect() -> None:
    unique_start_datetimes = set(
        [
            item
            for item in [
                item["properties"].get("start_datetime") for item in _all_items
            ]
            if item is not None
        ]
    )
    assert len(unique_start_datetimes) > 2
    unique_end_datetimes = set(
        [
            item
            for item in [item["properties"].get("end_datetime") for item in _all_items]
            if item is not None
        ]
    )
    assert len(unique_end_datetimes) > 2
    test_interval_start = sorted(list(unique_start_datetimes))[1]
    test_interval_end = sorted(list(unique_end_datetimes))[-2]
    expected_items: List[Dict[str, Any]] = []
    for item in _all_items:
        properties = item["properties"]
        if properties.get("start_datetime") is not None:
            if not (
                datetime.fromisoformat(properties["start_datetime"])
                >= datetime.fromisoformat(test_interval_end)
                or datetime.fromisoformat(properties["end_datetime"])
                <= datetime.fromisoformat(test_interval_start)
            ):
                expected_items.append(item)
    assert len(expected_items) > 0
    assert len(expected_items) < len(_all_items)
    compare_results_to_expected(
        expected_items,
        all_post_search_results(
            {
                "filter": {
                    "op": "or",
                    "args": [
                        {
                            "op": "t_intersects",
                            "args": [
                                {"property": "start_datetime"},
                                {
                                    "interval": [
                                        test_interval_start,
                                        test_interval_end,
                                    ]
                                },
                            ],
                        },
                        {
                            "op": "t_intersects",
                            "args": [
                                {"property": "end_datetime"},
                                {
                                    "interval": [
                                        test_interval_start,
                                        test_interval_end,
                                    ]
                                },
                            ],
                        },
                    ],
                },
            }
        ),
    )
