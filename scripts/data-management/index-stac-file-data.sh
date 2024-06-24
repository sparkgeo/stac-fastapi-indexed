#!/bin/bash

set -e

pushd $(dirname $0)/../..
index_dir=$(pwd)/data/index/s3
stac_index_index_output_dir=$index_dir python -m stac_indexer.index ./data/STAC/pc_partial_data/index-config.json
index_output_dir=$index_dir scripts/data-management/parquet-to-geoparquet.sh
