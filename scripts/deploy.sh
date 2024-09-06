#!/bin/bash

set -e

pushd $(dirname $0)/..

LOG_LEVEL="info"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --aws-account) AWS_ACCOUNT="$2"; shift ;;
        --aws-region) AWS_REGION="$2"; shift ;;
        --log-level) LOG_LEVEL="$2"; shift ;;
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

scripts/tests/integration-test.sh

pushd $(dirname $0)/../iac

cdk deploy \
    -c AWS_ACCOUNT=$AWS_ACCOUNT \
    -c AWS_REGION=$AWS_REGION \
    -c JWT_SECRET=${JWT_SECRET:-$(openssl rand -base64 32)} \
    -c LOG_LEVEL=${LOG_LEVEL} \
    -c BOTO_DEBUG=${BOTO_DEBUG:-false} \
    -c DUCKDB_THREADS=${DUCKDB_THREADS:-}
