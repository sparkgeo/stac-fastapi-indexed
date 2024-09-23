from setuptools import find_namespace_packages, setup

setup(
    name="stac-index.reader.https",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(),
    install_requires=[
        "aiohttp~=3.10.5",
    ],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
    },
)
