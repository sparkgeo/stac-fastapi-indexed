#!/bin/bash

set -e

pushd $(dirname $0)/..

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <root-http-catalog-uri>"
    exit 1
fi

root_catalog_uri="$1"

export tmp_index_config_path=$(mktemp)
echo "{\"root_catalog_uri\": \"$root_catalog_uri\", \"indexables\": {}, \"queryables\": {}, \"sortables\": {}}" > $tmp_index_config_path

dco="docker compose -f docker-compose.base.yml -f docker-compose.remote-http.yml"

$dco build
echo; echo "* Indexing may take some time, depending on the size of the catalog *"; echo
sleep 1
$dco up
