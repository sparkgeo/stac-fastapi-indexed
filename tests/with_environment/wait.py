from datetime import datetime, timezone
from os import environ
from time import sleep
from typing import Final

import requests

from .common import api_base_url

_healthcheck_url: Final[str] = f"{api_base_url}/_mgmt/ping"
_healthcheck_timeout_seconds: Final[int] = int(
    environ.get("API_HEALTHCHECK_TIMEOUT_SECONDS", 120)
)
_healthcheck_check_interval_seconds: Final[int] = 1


def wait_for_api() -> None:
    timer = datetime.now(tz=timezone.utc)
    while (
        datetime.now(tz=timezone.utc) - timer
    ).seconds < _healthcheck_timeout_seconds:
        try:
            requests.get(_healthcheck_url)
            print("API available, executing tests")
            return
        except Exception:
            print(f"waiting for API at {api_base_url}")
            sleep(_healthcheck_check_interval_seconds)
    raise Exception(
        f"API unavailable after {_healthcheck_timeout_seconds} seconds, failing tests"
    )
