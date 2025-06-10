#!/bin/bash

set -e

pushd $(dirname $0)/..

LOG_LEVEL="info"
EXECUTE_TESTS=1

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --aws-account) AWS_ACCOUNT="$2"; shift ;;
        --aws-region) AWS_REGION="$2"; shift ;;
        --log-level) LOG_LEVEL="$2"; shift ;;
        --root-catalog-uri) ROOT_CATALOG_URI="$2"; shift ;;
        --indexer-repeat-minutes) INDEXER_REPEAT_MINUTES="$2"; shift ;;
        --no-test) EXECUTE_TESTS=0 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [[ -z "$AWS_ACCOUNT" ]]; then
    echo "Error: --aws-account is required"
    exit 1
fi

if [[ -z "$AWS_REGION" ]]; then
    echo "Error: --aws-region is required"
    exit 1
fi

if [ $EXECUTE_TESTS -eq 1 ]; then
    scripts/tests/integration-test.sh
fi

pushd $(dirname $0)/../iac

cdk deploy \
    -c AWS_ACCOUNT=$AWS_ACCOUNT \
    -c AWS_REGION=$AWS_REGION \
    -c JWT_SECRET=${JWT_SECRET:-$(openssl rand -base64 32)} \
    -c LOG_LEVEL=${LOG_LEVEL} \
    -c DUCKDB_THREADS=${DUCKDB_THREADS:-} \
    -c ROOT_CATALOG_URI=${ROOT_CATALOG_URI:-} \
    -c INDEXER_REPEAT_MINUTES=${INDEXER_REPEAT_MINUTES:-}
