#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.yml -f docker-compose.local-sample.yml -f docker-compose.cite.yml"

$dco build
$dco up --force-recreate
