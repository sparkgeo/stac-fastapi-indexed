# STAC API Serverless

A stac-fastapi backend that indexes a static catalog to Parquet to make it searchable. The ability to read the Parquet index files remotely mean this backend can run in a serverless environment.

This backend does not support transactions and accesses Parquet index files read-only.

## Known Issues

See [Known Issues](./KNOWN-ISSUES.md) for more information.

## CITE Compliance

See [here](./CITE-TESTS.md) to execute CITE compliance tests.
