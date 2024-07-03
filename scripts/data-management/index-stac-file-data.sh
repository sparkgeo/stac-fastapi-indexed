#!/bin/bash

set -e

# ensure Docker is running before embarking on a potentially long journey
docker ps -q

pushd $(dirname $0)/../..
index_dir=$(pwd)/data/index/s3

stac_index_indexer_output_dir=$index_dir \
stac_index_indexer_log_level=debug \
    python -m stac_index.indexer.index ./data/STAC/pc_partial_data/index-config.json
index_output_dir=$index_dir scripts/data-management/parquet-to-geoparquet.sh
