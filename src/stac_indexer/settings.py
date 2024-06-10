from functools import lru_cache
from os import path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_index_",
    )
    log_level: str = "info"
    test_collection_item_limit: Optional[int] = None
    index_output_dir: str = path.join(path.dirname(__file__), "index_data")


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
