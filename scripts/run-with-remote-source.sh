#!/bin/bash

set -e

pushd $(dirname $0)/..

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <root-catalog-uri>"
    exit 1
fi

export root_catalog_uri="$1"
if [[ $root_catalog_uri == s3://* ]]; then
    echo; echo "* Assumes \$AWS_ACCESS_KEY_ID, \$AWS_REGION, \$AWS_SECRET_ACCESS_KEY, and (optionally) \$AWS_SESSION_TOKEN are set for obstore *"; echo
fi

export tmp_index_config_path=$(mktemp)
if [ -z "${FIXES_TO_APPLY}" ]; then
    echo "{}" > $tmp_index_config_path
else
    fixes_json=$(echo "${FIXES_TO_APPLY}" | sed "s/,\s*/\", \"/g")
    echo "{\"fixes_to_apply\": [\"${fixes_json}\"]}" > $tmp_index_config_path
fi

dco="docker compose -f docker-compose.base.yml -f docker-compose.remote-source.yml"

$dco build
echo; echo "* Indexing may take some time, depending on the size of the catalog *"; echo
sleep 1
$dco up --force-recreate
