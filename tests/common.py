import sys
from uuid import uuid4

from pytest import MonkeyPatch


def monkeypatch_settings(monkeypatch: MonkeyPatch, **kwargs):
    for module_name in ("stac_fastapi.indexed.settings",):
        if module_name in sys.modules:
            del sys.modules[module_name]
    default_settings = {
        "stac_api_indexed_token_jwt_secret": uuid4().hex,
    }
    for key, value in {**default_settings, **kwargs}.items():
        if value is None:
            monkeypatch.delenv(key)
        else:
            monkeypatch.setenv(key, value)
