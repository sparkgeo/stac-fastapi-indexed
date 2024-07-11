#!/bin/bash

set -e

pushd $(dirname $0)/..

sh scripts/tests/smoke-test.sh

pushd $(dirname $0)/../iac

cdk deploy -c PARQUET_URI=PARQUET_URI -c JWT_SECRET=$(openssl rand -base64 32) -c LOG_LEVEL=${LOG_LEVEL:-info} -c BOTO_DEBUG=${BOTO_DEBUG:-false} -c DUCKDB_THREADS=${DUCKDB_THREADS:-}





