#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml -f docker-compose.remote-s3.yml"

$dco build
$dco up --force-recreate
