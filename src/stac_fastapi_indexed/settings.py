from functools import lru_cache

from stac_fastapi.types.config import ApiSettings, SettingsConfigDict


class _Settings(ApiSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_api_indexed_",
    )
    log_level: str = "info"
    parquet_index_source_url: str
    token_jwt_secret: str
    permit_boto_debug: bool = False


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
