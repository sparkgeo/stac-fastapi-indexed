from setuptools import find_packages, setup

setup(
    name="stac-index-common",
    version="0.1.0",
    python_requires=">=3.12",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[],
    extras_require={
        "dev": [
            "pre-commit>=3.6.2,<4.0",
        ],
    },
)
