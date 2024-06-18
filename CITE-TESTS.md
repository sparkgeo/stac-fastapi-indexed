# Executing CITE Compliance Tests

## Setup

Start all relevant containers (leave the script running).

```sh
scripts/run-cite-tests.sh
```

## Execute

Navigate to http://localhost:8080/teamengine/

Login with `ogctest/ogctest`.

Provide `http://api` as the landing page location.

## Cleanup

`Ctrl+C` out of the script executed in [Setup](#setup).
