#!/bin/bash

set -e

root_catalog_uri_argument=""
manifest_json_uri_argument=""
index_config_argument=""
publish_to_uri_argument=""

if [ -n "$INDEX_ROOT_CATALOG_URI" ]; then
    root_catalog_uri_argument="--root_catalog_uri $INDEX_ROOT_CATALOG_URI"
fi

if [ -n "$INDEX_MANIFEST_JSON_URI" ]; then
    manifest_json_uri_argument="--manifest_json_uri $INDEX_MANIFEST_JSON_URI"
fi

if [ -n "$INDEX_CONFIG_PATH" ] && [ -f "$INDEX_CONFIG_PATH" ]; then
    index_config_argument="--index_config $INDEX_CONFIG_PATH"
fi

if [ -n "$INDEX_PUBLISH_PATH" ]; then
    publish_to_uri_argument="--publish_to_uri $INDEX_PUBLISH_PATH"
fi

python -m stac_index.indexer.index \
    $root_catalog_uri_argument \
    $manifest_json_uri_argument \
    $index_config_argument \
    $publish_to_uri_argument
