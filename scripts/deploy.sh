#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml -f docker-compose.local-s3.yml -f docker-compose.local-s3.smoke-test.yml"

$dco build
$dco run tester
$dco logs
$dco down

pushd $(dirname $0)/../iac

cdk deploy -c PARQUET_URI=PARQUET_URI -c JWT_SECRET=$(openssl rand -base64 32) -c LOG_LEVEL=${LOG_LEVEL:-info} -c BOTO_DEBUG=${BOTO_DEBUG:-false} -c DUCKDB_THREADS=${DUCKDB_THREADS:-}
