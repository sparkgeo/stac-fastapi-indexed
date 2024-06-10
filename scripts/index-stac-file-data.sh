#!/bin/bash

set -e

pushd $(dirname $0)/..
python -m stac_indexer.index file://$(pwd)/data/STAC/pc_partial_data/catalog.json
popd
