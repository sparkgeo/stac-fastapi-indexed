# Development

Development requires Python >= 3.12, Docker, and Bash.

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
```

# Deployment

This project does not currently support Continuous Deployment. Deployments are automated via AWS CDK but must be initiated manually.


```sh
# --aws-account and --aws-region are always required
scripts/deploy.sh --aws-account 012345678901 --aws-region us-west-2

# --log-level may also be specified
scripts/deploy.sh --aws-account 012345678901 --aws-region us-west-2 --log-level debug
```
