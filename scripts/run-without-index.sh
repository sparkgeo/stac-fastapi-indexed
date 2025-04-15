#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml"

$dco build
create_empty_index_if_missing=true $dco up --force-recreate
