from os import environ
from typing import Final

api_base_url: Final[str] = environ["API_ROOT_PATH"]
stac_json_root_dir: Final[str] = environ["STAC_JSON_ROOT_DIR"]
