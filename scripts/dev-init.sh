#!/bin/bash

set -e

pushd $(dirname $0)/..

pip install -e ./stac_index/common[dev]
pip install -e ./stac_index/reader/s3[dev]
pip install -e ./stac_index/indexer[dev]
pip install -e ./stac_fastapi/indexed[dev,server,test]

pre-commit install
