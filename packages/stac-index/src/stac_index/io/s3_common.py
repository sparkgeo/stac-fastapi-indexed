from functools import lru_cache
from re import match
from typing import Final, Optional

from obstore.store import S3Store
from pydantic_settings import BaseSettings, SettingsConfigDict

uri_prefix_regex: Final[str] = r"^s3://"


def can_handle_uri(uri: str) -> bool:
    return not not match(uri_prefix_regex, uri)


def path_separator() -> str:
    return "/"


def obstore_for_bucket(bucket: str) -> S3Store:
    client_config = {}
    store_config = {}
    s3_endpoint = get_settings().endpoint
    if s3_endpoint is not None:
        store_config["endpoint"] = s3_endpoint
        if s3_endpoint.startswith("http://"):
            client_config["allow_http"] = True
    return S3Store(bucket=bucket, config=store_config, client_options=client_config)


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="stac_index_reader_s3_",
    )
    log_level: str = "info"
    endpoint: Optional[str] = None


@lru_cache(maxsize=1)
def get_settings() -> _Settings:
    return _Settings()
