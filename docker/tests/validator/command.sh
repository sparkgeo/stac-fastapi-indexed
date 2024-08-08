#!/bin/bash

set -e

/scripts/wait_for_api.sh

stac-api-validator \
    --root-url $API_ROOT_PATH \
    --conformance core \
    --conformance collections \
    --conformance features \
    --conformance item-search \
    --conformance filter \
    --collection joplin \
    --geometry '{"type": "Polygon", "coordinates": [[[-180,-90], [180,-90], [180,90], [-180,90], [-180,-90]]]}'
