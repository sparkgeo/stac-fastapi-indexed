from setuptools import find_namespace_packages, setup

setup(
    name="stac-index.indexer",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(where="src/", include=["stac_index.indexer"]),
    package_dir={"": "src"},
    install_requires=[
        "duckdb~=1.1.3",
        "pydantic-settings",  # don't specify a version, determined by stac-pydantic
        "shapely~=2.0.4",
        "stac-fastapi.types~=3.0.0a2",
        "stac-pydantic~=3.1.0",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
        "test": [
            "pytest~=8.2.2",
        ],
        "local-deps": [  # managed separately to support generation of pypi-only requirements.txt files for #88
            "stac-index.common==0.1.0",
        ],
    },
)
