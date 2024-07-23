#!/bin/bash

set -e

pushd $(dirname $0)/..

dco="docker compose -f docker-compose.base.yml -f docker-compose.local-s3.yml -f docker-compose.local-s3.test.yml"

$dco build
$dco run --rm tester python -m pytest -k smoke_tests
exit_code=$?
if [ $exit_code -ne 0 ] || [ ${TESTS_DEBUG:-0} -ne 0 ]; then
    $dco logs
fi
$dco down

pushd $(dirname $0)/../iac

cdk deploy -c PARQUET_URI=$PARQUET_URI -c JWT_SECRET=$(openssl rand -base64 32) -c LOG_LEVEL=${LOG_LEVEL:-info} -c BOTO_DEBUG=${BOTO_DEBUG:-false} -c DUCKDB_THREADS=${DUCKDB_THREADS:-}
