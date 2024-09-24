from setuptools import find_namespace_packages, setup

setup(
    name="stac-fastapi.indexed",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(),
    install_requires=[
        "asgi-correlation-id~=4.3.3",
        "duckdb~=1.1.0",
        "orjson",
        "pydantic-settings",  # don't specify a version, determined by stac-pydantic
        "pygeofilter[backend-native]==0.2.1",
        "pyjwt~=2.8.0",
        "pytz",
        "shapely~=2.0.4",
        "stac-fastapi.api~=3.0.0",
        "stac-fastapi.extensions~=3.0.0",
        "stac-fastapi.types~=3.0.0",
        "stac-pydantic~=3.1.0",
    ],
    extras_require={
        "dev": [
            "pip-tools~=7.4.1",
            "pre-commit>=3.6.2,<4.0",
        ],
        "local-deps": [  # managed separately to support generation of pypi-only requirements.txt files for #88
            "stac-index.common==0.1.0",
        ],
        "server": [
            "uvicorn~=0.30.1",
        ],
        "lambda": [
            "mangum~=0.17.0",
        ],
    },
)
