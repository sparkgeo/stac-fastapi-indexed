#!/bin/bash

set -e

pushd $(dirname $0)/..

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <root-catalog-uri>"
    exit 1
fi

if [ -z "${FIXES_TO_APPLY}" ]; then
    fixes_json=""
else
    fixes_json=$(echo "${FIXES_TO_APPLY}" | sed "s/,/\", \"/g")
    fixes_json=", \"fixes_to_apply\": [\"${fixes_json}\"]"
fi


root_catalog_uri="$1"

export tmp_index_config_path=$(mktemp)
echo "{\"root_catalog_uri\": \"$root_catalog_uri\", \"indexables\": {}, \"queryables\": {}, \"sortables\": {} $fixes_json }" > $tmp_index_config_path
dco="docker compose -f docker-compose.base.yml -f docker-compose.remote-source.yml"

$dco build
echo; echo "* Indexing may take some time, depending on the size of the catalog *"; echo
sleep 1
$dco up
