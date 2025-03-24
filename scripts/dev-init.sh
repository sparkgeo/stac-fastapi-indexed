#!/bin/bash

set -e

pushd $(dirname $0)/..

uv sync --extra dev --extra server --extra test
pip install -r ./iac/requirements.txt

uv run pre-commit install
