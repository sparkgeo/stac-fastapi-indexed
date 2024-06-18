#!/bin/bash

set -e

pushd $(dirname $0)/../../data/STAC
python -m pc_partial_download
popd
