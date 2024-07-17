from glob import glob
from json import dumps
from os import environ, path
from typing import Any, Dict, Final, List

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
