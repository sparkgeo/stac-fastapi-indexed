#!/bin/bash

pushd $(dirname $0)/../..

dco="docker compose -f docker-compose.yml -f docker-compose.local-s3.yml -f docker-compose.local-s3.smoke-test.yml"

$dco build
$dco run tester
exit_code=$?
$dco down
exit $exit_code
