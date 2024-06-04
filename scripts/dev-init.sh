#!/bin/bash

set -e

pip install .[dev]

pre-commit install
