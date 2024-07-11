from glob import glob
from os import environ, path
from typing import Any, Dict, Final, List

stac_json_root_dir: Final[str] = environ["STAC_JSON_ROOT_DIR"]


def get_link_hrefs_by_rel(data_dict: Dict[str, Any], rel_type: str) -> List[str]:
    assert "links" in data_dict
    return [link["href"] for link in data_dict["links"] if link["rel"] == rel_type]


def get_collection_file_paths():
    return glob(path.join(stac_json_root_dir, "collections", "*.json"))


def get_item_file_paths_for_collection(collection_id: str):
    return glob(
        path.join(stac_json_root_dir, "collections", collection_id, "items", "*.json")
    )
