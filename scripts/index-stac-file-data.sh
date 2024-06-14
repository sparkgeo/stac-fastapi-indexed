#!/bin/bash

set -e

pushd $(dirname $0)/..
index_dir=$(pwd)/data/index/s3
stac_index_index_output_dir=$index_dir python -m stac_indexer.index s3://tchristian-stac-serverless-data/pc_partial_data/catalog.json
index_output_dir=$index_dir scripts/parquet-to-geoparquet.sh
