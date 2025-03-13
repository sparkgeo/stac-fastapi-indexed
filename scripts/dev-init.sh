#!/bin/bash

set -e

pushd $(dirname $0)/..

pip install -e ./stac_index/common[dev]
pip install -e ./stac_index/reader/filesystem[dev]
pip install -e ./stac_index/reader/https[dev]
pip install -e ./stac_index/reader/s3[dev]
pip install -e ./stac_index/indexer[dev,test]
pip install -e ./stac-fastapi.indexed[dev,server]
pip install -r ./iac/requirements.txt

pre-commit install
