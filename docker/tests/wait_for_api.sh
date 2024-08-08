#!/bin/bash

timeout=${API_HEALTHCHECK_TIMEOUT_SECONDS:-120}

start_time=$(date +%s)
while true; do

    if ! curl -s -o /dev/null --fail "$API_HEALTHCHECK"; then
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))
        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "API failed to start within $timeout seconds"
            exit 1
        else
            echo "waiting for API..."
            sleep 1
        fi
    else
        echo "API ready"
        break
    fi
done
