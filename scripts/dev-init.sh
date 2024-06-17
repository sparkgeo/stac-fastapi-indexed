#!/bin/bash

set -e

pip install .[dev,server]

pre-commit install
