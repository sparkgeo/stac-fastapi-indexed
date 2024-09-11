#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml -f docker-compose.local-file.yml"

$dco build
$dco up --force-recreate
