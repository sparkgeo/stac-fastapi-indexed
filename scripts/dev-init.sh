#!/bin/bash

set -e

pip install -e stac_index_common[dev]
pip install -e stac_fastapi_indexed[dev,server,s3_source]
pip install -e stac_indexer[dev,s3_source]

pre-commit install
