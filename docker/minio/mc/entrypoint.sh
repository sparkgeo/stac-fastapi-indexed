#!/bin/bash

set -e

mc alias set minio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc mb --ignore-existing minio/stac
mc mb --ignore-existing minio/index

exec "$@"
