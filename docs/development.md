# Development

Development requires Python >= 3.12, Docker, Bash, and uv.

## Configure Local Environment

```sh
scripts/dev-init.sh
```

## Dependencies

This project uses uv as the package manager, with `pyproject.toml` and `uv.lock` files to manage dependencies and versions. To add a package to the API, run `uv add <package>` in the root directory. For dependency packages, run the same command in their respective directories. If you manually edit `pyproject.toml`, the lock file will also need to be updated by running uv sync and include the optional packages (example: `uv sync --extra dev --extra server --extra test`).


## Testing

This project is currently well covered by integration tests but not by unit tests.

Any new functionality should extend integration tests as a first priority. Unit tests should be extended as resource availability permits as a secondary priority.

### Execute Tests

```sh
scripts/tests/unit-test.sh
scripts/tests/smoke-test.sh
scripts/tests/integration-test.sh
# hang after each test run to permit interaction with test infrastructure and test debugging
scripts/tests/integration-test.sh --debug
# dump container logs after each test run
scripts/tests/integration-test.sh --dump-log
```

# Deployment

This project does not currently support Continuous Deployment. Deployments are automated via AWS CDK but must be initiated manually.

> [!NOTE]
> The indexer's "first run" below refers to an indexer execution with no `manifest.json` at the default publish URI. The default publish URI is `s3://<deployment-data-bucket>/index/manifest.json`. The first execution of a newly-deployed indexer will be its first run, and you can recreate first run behaviour by deleting `manifest.json` before a run.

```sh
# --aws-account and --aws-region are always required
# --root-catalog-uri must be provided for the indexer's first run, but may be omitted or included thereafter. Changes to --root-catalog-uri after the first run will have no effect
# --indexer-repeat-minutes is required to configure a cron-style event that triggers the indexer to run repeatedly. If omitted the indexer must be executed manually
# --no-test will skip execution of integration tests prior to deployment. This option should only be used when iteratively debugging deployment issues
# --log-level is optional and defaults to 'info'
scripts/deploy.sh --aws-account 012345678901 --aws-region us-west-2 --root-catalog-uri s3://... --indexer-repeat-minutes 1440
```
