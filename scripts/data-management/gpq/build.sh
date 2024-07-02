#!/bin/bash

set -e

pushd $(dirname $0)/../..

export gpq_image_name=sparkgeo/gpq:0.22.0

docker build \
    -t $gpq_image_name \
    -f scripts/data-management/gpq/Dockerfile \
    scripts/data-management/gpq
