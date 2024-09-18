# Development

Development requires Python >= 3.12, Docker, and Bash.

## Configure Local Environment

```sh
scripts/dev-init.sh
```

## Testing

This project is currently well covered by integration tests but not by unit tests.

Any new functionality should extend integration tests as a first priority. Unit tests should be extended as resource availability permits as a secondary priority.

### Execute Tests

```sh
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
