#!/bin/bash

pushd $(dirname $0)/../..

dco="docker compose -f docker-compose.base.yml -f docker-compose.local-s3.yml -f docker-compose.local-s3.validator-test.yml"

$dco build
$dco run --rm validator
exit_code=$?
if [ $exit_code -ne 0 ] || [ ${TESTS_DEBUG:-0} -ne 0 ]; then
    $dco logs
fi
$dco down
exit $exit_code
