from base64 import b64decode, b64encode
from datetime import datetime
from glob import glob
from json import dumps, load, loads
from os import environ, path
from typing import Any, Dict, Final, List, Optional, Tuple

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
        if "features" not in response:
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
        if "features" not in response:
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


def get_claims_from_token(token: str) -> Dict[str, Any]:
    token_parts = token.split(".")
    assert len(token_parts) == 3
    claims_part = token_parts[1]
    missing_padding = len(claims_part) % 4
    if missing_padding:
        claims_part += "=" * (4 - missing_padding)
    decoded_bytes = b64decode(claims_part)
    return loads(decoded_bytes.decode("UTF-8"))


def rebuild_token_with_altered_claims(
    original_token: str, altered_claims: Dict[str, Any]
) -> str:
    token_parts = original_token.split(".")
    assert len(token_parts) == 3
    return "{}.{}.{}".format(
        token_parts[0],
        b64encode(dumps(altered_claims).encode("UTF-8")).decode("UTF-8"),
        token_parts[2],
    )


def get_test_items() -> (
    Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]
):
    all_collections: List[Dict[str, Any]] = []
    all_items_by_collection_id: Dict[str, List[Dict[str, Any]]] = {}
    all_items: List[Dict[str, Any]] = []
    for collection_file_path in get_collection_file_paths():
        with open(collection_file_path, "r") as f:
            collection = load(f)
            all_collections.append(collection)
        all_items_by_collection_id[collection["id"]] = []
        for item_file_path in get_item_file_paths_for_collection(collection["id"]):
            with open(item_file_path, "r") as f:
                all_items_by_collection_id[collection["id"]].append(load(f))
    all_items.extend(
        [item for sublist in all_items_by_collection_id.values() for item in sublist]
    )
    all_items.sort(key=lambda x: "{}_{}".format(x["collection"], x["id"]))
    return (all_collections, all_items_by_collection_id, all_items)
