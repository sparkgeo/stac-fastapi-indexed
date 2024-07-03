from setuptools import find_namespace_packages, setup

setup(
    name="stac-index.common",
    version="0.1.0",
    python_requires=">=3.12",
    packages=find_namespace_packages(),
    install_requires=[],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
    },
)
