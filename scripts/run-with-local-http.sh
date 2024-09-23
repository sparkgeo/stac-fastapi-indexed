#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml -f docker-compose.local-http.yml"

$dco build
$dco up --force-recreate
