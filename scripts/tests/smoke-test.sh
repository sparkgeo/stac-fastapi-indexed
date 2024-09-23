#!/bin/bash

pushd $(dirname $0)/../..

stack_commands=(
    "docker compose -f docker-compose.base.yml -f docker-compose.local-s3.yml -f docker-compose.tester.yml"
    "docker compose -f docker-compose.base.yml -f docker-compose.local-file.yml -f docker-compose.tester.yml"
    "docker compose -f docker-compose.base.yml -f docker-compose.local-http.yml -f docker-compose.tester.yml"
)

exit_code=0

for dco in "${stack_commands[@]}"; do
    if [ $exit_code -ne 0 ]; then
        break
    fi
    $dco build
    $dco run --rm tester python -m pytest -k smoke_tests
    exit_code=$?
    if [ $exit_code -ne 0 ] || [ ${TESTS_DEBUG:-0} -ne 0 ]; then
        $dco logs
    fi
    $dco down
done

exit $exit_code
