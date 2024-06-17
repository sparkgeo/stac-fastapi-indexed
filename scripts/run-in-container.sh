#!/bin/bash

set -e

pushd $(dirname $0)/..

docker compose build
docker compose up
