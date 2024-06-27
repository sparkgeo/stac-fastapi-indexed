from setuptools import find_packages, setup

setup(
    name="stac_fastapi_indexed",
    version="0.1.0",
    python_requires=">=3.12",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "duckdb~=1.0.0",
        "orjson",
        "pydantic-settings",  # don't specify a version, determined by stac-pydantic
        "pyjwt~=2.8.0",
        "pytz",
        "shapely~=2.0.4",
        "stac-fastapi.api~=3.0.0a2",
        "stac-fastapi.extensions~=3.0.0a2",
        "stac-fastapi.types~=3.0.0a2",
        "stac-pydantic~=3.1.0",
        "stac_index_common",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
        "server": [
            "uvicorn~=0.30.1",
        ],
        "s3_source": [
            "boto3~=1.34.125",
        ],
        "lambda": [
            "mangum~=0.17.0",
        ],
    },
)
