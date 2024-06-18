from setuptools import find_packages, setup

setup(
    name="stac_indexer",
    version="0.1.0",
    python_requires=">=3.12",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "duckdb~=1.0.0",
        "pydantic-settings",  # don't specify a version, determined by stac-pydantic
        "shapely~=2.0.4",
        "stac-fastapi.types~=3.0.0a2",
        "stac-pydantic~=3.1.0",
        "stac_index_common",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
        "s3_source": [
            "boto3~=1.34.125",
        ],
    },
)
