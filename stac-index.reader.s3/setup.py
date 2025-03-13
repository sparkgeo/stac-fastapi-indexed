from setuptools import find_namespace_packages, setup

setup(
    name="stac-index.reader.s3",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(where="src/", include=["stac_index.reader.s3"]),
    package_dir={"": "src"},
    install_requires=[
        "boto3~=1.34.125",
        "pydantic-settings~=2.0",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
    },
)
