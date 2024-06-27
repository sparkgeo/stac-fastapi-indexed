#!/bin/bash

echo "$stac_api_indexed_parquet_index_source_url"
cd iac
source .venv/bin/activate
cdk bootstrap
cdk deploy