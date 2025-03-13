#!/bin/bash

set -e

pushd $(dirname $0)/../..

python -m pytest stac*
