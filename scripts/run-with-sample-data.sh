#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.yml -f docker-compose.local-sample.yml"

$dco build
$dco up --force-recreate
