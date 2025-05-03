from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_index_indexer_",
    )
    log_level: str = "info"
    test_collection_item_limit: Optional[int] = None
    test_collection_limit: Optional[int] = None
    max_concurrency: int = 10


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
