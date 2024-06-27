#!/bin/bash

cd iac
source .venv/bin/activate
cdk bootstrap
cdk deploy