#!/bin/bash

set -e

pushd $(dirname $0)/..

. scripts/gpq/build.sh
input_dir=$(pwd)/src/stac_indexer/index_data/parquet
output_dir=$(pwd)/src/stac_indexer/index_data/geoparquet
mkdir -p $output_dir

geo_filenames=("items.parquet")
for geo_filename in "${geo_filenames[@]}"; do
    echo "$geo_filename converting to geoparquet"
    docker run \
        --rm \
        -v $input_dir:/input:ro \
        -v $output_dir:/output:rw \
        $gpq_image_name \
        gpq convert /input/$geo_filename /output/$geo_filename
    echo "$geo_filename validating"
    docker run \
        --rm \
        -v $output_dir:/input:ro \
        $gpq_image_name \
        gpq validate /input/$geo_filename
done
for filepath in $input_dir/*.parquet; do
    filename=$(basename $filepath)
    copy=1
    for geo_filename in "${geo_filenames[@]}"; do
        if [ "$filename" == "$geo_filename" ]; then
            copy=0
        fi
    done
    if [ $copy -eq 1 ]; then
        echo "$filename copying without conversion"
        cp $filepath $output_dir/
    fi
done

ls -Alh $output_dir
