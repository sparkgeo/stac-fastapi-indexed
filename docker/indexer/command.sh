#!/bin/bash

set -e

input_index_config_path="${INDEX_CONFIG_PATH:-/index-config.json}"
output_index_config_path=/opt/active-config.json

if [ -n "$INDEX_CATALOG_ROOT_PATH" ]; then
    jq ".root_catalog_uri |= \"$INDEX_CATALOG_ROOT_PATH\"" $input_index_config_path > $output_index_config_path
else
    cp $input_index_config_path $output_index_config_path
fi

python -m stac_index.indexer.index "$output_index_config_path"
