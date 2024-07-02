from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_index_reader_s3_",
    )
    log_level: str = "info"
    endpoint: Optional[str] = None
    permit_boto_debug: Optional[bool] = False


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
