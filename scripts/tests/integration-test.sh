#!/bin/bash

pushd $(dirname $0)/../..

hang=0
dump_log=0
for arg in "$@"; do
    if [ "$arg" == "--debug" ]; then
        hang=1
    fi
    if [ "$arg" == "--dump-log" ]; then
        dump_log=1
    fi
done

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
    build_result=$?
    if [ $build_result -ne 0 ]; then
        exit $build_result
    fi
    $dco run --rm tester python -m pytest -k integration_tests
    exit_code=$?
    if [ $hang -eq 1 ]; then
        echo; echo "  ...stack will stay up for debugging support until you hit any key (except the 'any' key)"; echo
        read -n 1 -s
    fi
    if [ $dump_log -eq 1 ]; then
        $dco logs
    fi
    $dco down --volumes
done

exit $exit_code
