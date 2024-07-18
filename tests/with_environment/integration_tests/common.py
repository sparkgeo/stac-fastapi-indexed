from datetime import datetime
from glob import glob
from json import dumps
from os import environ, path
from typing import Any, Dict, Final, List, Optional

import requests
from with_environment.common import api_base_url

stac_json_root_dir: Final[str] = environ["STAC_JSON_ROOT_DIR"]


def get_link_hrefs_by_rel(data_dict: Dict[str, Any], rel_type: str) -> List[str]:
    return [link["href"] for link in get_link_dict_by_rel(data_dict, rel_type)]


def get_link_dict_by_rel(
    data_dict: Dict[str, Any], rel_type: str
) -> List[Dict[str, Any]]:
    assert "links" in data_dict
    return [link for link in data_dict["links"] if link["rel"] == rel_type]


def get_collection_file_paths():
    return glob(path.join(stac_json_root_dir, "collections", "*.json"))


def get_item_file_paths_for_collection(collection_id: str):
    return glob(
        path.join(stac_json_root_dir, "collections", collection_id, "items", "*.json")
    )


def get_index_config_path() -> str:
    return path.join(stac_json_root_dir, "..", "index-config.json")


def compare_results_to_expected(
    expected_results: List[Dict[str, Any]], actual_results: List[Dict[str, Any]]
):
    assert len(actual_results) == len(expected_results)
    for expected_result in expected_results:
        # exclude links from comparison as they are modified by the API
        expected_result_json = dumps({**expected_result, "links": []})
        found = False
        for result in actual_results:
            if expected_result_json == dumps({**result, "links": []}):
                found = True
        assert found


def all_post_search_results(post_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    last_search_data = None
    search_data: Optional[Dict[str, Any]] = post_data.copy()
    while search_data is not None:
        response = requests.post(f"{api_base_url}/search", json=search_data).json()
        if not "features" in response:
            raise Exception("unexpected response: ", response)
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


def all_get_search_results(query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    last_url = None
    search_url: Optional[str] = f"{api_base_url}/search"
    search_data: Optional[Dict[str, Any]] = query_params.copy()
    while search_url is not None:
        response = requests.get(search_url, params=search_data).json()
        if not "features" in response:
            raise Exception("unexpected response: ", response)
        all_results.extend(response["features"])
        next_links = get_link_dict_by_rel(response, "next")
        if len(next_links) == 1:
            last_url = search_url
            search_url = next_links[0]["href"]
            if last_url == search_url:
                raise Exception("next search link is not advancing")
            search_data = None
        else:
            search_url = None
    return all_results


def get_items_with_intersecting_datetime(
    item_set: List[Dict[str, Any]], comparison_datetime: datetime
) -> List[Dict[str, Any]]:
    intersecting_set: List[Dict[str, Any]] = []
    for item in item_set:
        properties = item["properties"]
        if properties["datetime"] is None:
            if (
                properties["start_datetime"] <= comparison_datetime
                and properties["end_datetime"] >= comparison_datetime
            ):
                intersecting_set.append(item)
        else:
            if properties["datetime"] == comparison_datetime:
                intersecting_set.append(item)
    return intersecting_set
