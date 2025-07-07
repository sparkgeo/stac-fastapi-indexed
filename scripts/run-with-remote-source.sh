#!/bin/bash

set -e

pushd $(dirname $0)/..

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <root-catalog-uri>"
    exit 1
fi

export root_catalog_uri="$1"

if [[ $root_catalog_uri == s3://* ]]; then
    echo; echo "* Assumes \$AWS_ACCESS_KEY_ID, \$AWS_REGION, \$AWS_SECRET_ACCESS_KEY, and (optionally) \$AWS_SESSION_TOKEN are set for obstore *"; echo
fi

export tmp_index_path=$PWD/.remote-source-index/$(echo "$root_catalog_uri" | tr -cd '[:alnum:]')
echo; echo "* Indexing may take some time, depending on the size of the catalog";
echo "* Indexing to $tmp_index_path"; echo
# Persist generated index and manifest files locally to support faster repeat runs against the same remote source.
if [ -f $"$tmp_index_path/manifest.json" ]; then
    # Tell the indexer there's already an existing index to update.
    export index_manifest_json_uri="/output/manifest.json"
    unset root_catalog_uri
else
    # No point evaluating this if updating an existing index as it will be ignored.
    if [ -n "${FIXES_TO_APPLY}" ]; then
        export tmp_index_config_path=$(mktemp)
        fixes_json=$(echo "${FIXES_TO_APPLY}" | sed "s/,\s*/\", \"/g")
        echo "{\"fixes_to_apply\": [\"${fixes_json}\"]}" > $tmp_index_config_path
    fi
fi


dco="docker compose -f docker-compose.base.yml -f docker-compose.remote-source.yml"
$dco build
$dco up --force-recreate
