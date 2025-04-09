from functools import lru_cache
from typing import Optional

from stac_fastapi.types.config import ApiSettings, SettingsConfigDict


class _Settings(ApiSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_api_indexed_",
    )
    log_level: str = "info"
    index_manifest_uri: str
    token_jwt_secret: str
    s3_endpoint: Optional[str] = None
    duckdb_threads: Optional[int] = None
    deployment_root_path: Optional[str] = None
    install_duckdb_extensions: bool = (
        True  # container images set this to false after installing extensions in build
    )


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
