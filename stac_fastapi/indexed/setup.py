from setuptools import find_namespace_packages, setup

setup(
    name="stac-fastapi.indexed",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(),
    install_requires=[
        "duckdb~=1.0.0",
        "orjson",
        "pydantic-settings",  # don't specify a version, determined by stac-pydantic
        "pygeofilter[backend-native]==0.2.1",
        "pyjwt~=2.8.0",
        "pytz",
        "shapely~=2.0.4",
        "stac-fastapi.api~=3.0.0b2",
        "stac-fastapi.extensions~=3.0.0b2",
        "stac-fastapi.types~=3.0.0b2",
        "stac-pydantic~=3.1.0",
        "stac-index.common",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
        "server": [
            "uvicorn~=0.30.1",
        ],
        "lambda": [
            "mangum~=0.17.0",
        ],
        "test": [
            "pytest~=8.2.2",
            "requests~=2.32.3",
            "shapely~=2.0.4",
        ],
    },
)
