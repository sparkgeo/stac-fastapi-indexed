# Suitability

This document explores questions that may determine whether this project is suitable for a given use-case. This project assumes that a number of limitations are acceptable trade-offs for a solution with lower operating costs and which can add STAC API behaviour to existing static STAC catalogs.

## Known Limitations

### Transactions

This stac-fastapi backend does not support transactions and never will. If you need the ability to modify data through the API consider using [stac-fastapi-pgstac](https://github.com/stac-utils/stac-fastapi-pgstac) or other projects that support transactions.

### Data Duplication

The indexing approach requires that some STAC data be duplicated in Parquet index files, and these files are the API's source of truth about that STAC data. If the STAC data changes and the Parquet index files are not updated before an API request is received it is possible for the API to return incorrect data or an error.

This risk can be mitigated somewhat by a shorter indexer repeat cycle, or by event-driven item updates as are intended by [#157](https://github.com/sparkgeo/stac-fastapi-indexed/issues/157), but the risk cannot be eliminated entirely.

[#160](https://github.com/sparkgeo/stac-fastapi-indexed/issues/160) could also help to address some data duplication risks.

### Performance

Efforts have been made to optimise performance in this project, however not all performance risks can be addressed.

The indexer can be used to index STAC data hosted on third-party infrastructure. Both the indexer's **and** the API's runtime performance can be impacted by performance issues affecting that infrastructure. Similarly, any availability issues affecting third-party infrastructure will affect the indexer and the API.

The need for the API to fetch content from the STAC data source may also impact performance. Network requests to S3 storage, HTTPS URIs, or other non-local data stores may adversely impact performance.

#### Metrics

Benchmarking during development shows reasonable performance.

For a small STAC catalog (1 collection, <100 items) where both STAC data and Parquet index files are stored in S3, deployed using AWS API Gateway and Lambda with a "warm" instance available, basic search requests have completed in 600-700 milliseconds.

For a large STAC catalog (>20 collections, >1,700,000 items) in the same deployment configuration basic search requests have completed in 1.5-2 seconds.

#### Index Location

If necessary, performance can be improved by accessing Parquet index files locally on disk. This could be achieved by building the files directly in the container image or with a filesystem mount.

## Alternatives

A number of alternative STAC API strategies are available that might avoid some or all of this project's [known limitations](#known-limitations).

### Data Management Platforms

Several solutions exist that rely on data management platforms such as [stac-fastapi-pgstac](https://github.com/stac-utils/stac-fastapi-pgstac) or [stac-fastapi-elasticsearch-opensearch](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch). These solutions treat their data store as the sole source of truth and the API _should_ guarantee accurate responses.

### STAC-GeoParquet

STAC-GeoParquet is a specification for storing STAC data in GeoParquet files. Its primary advantage is that large amounts of STAC data can be queried quickly thanks to the analysis-friendly nature of Parquet and some excellent tooling that is compatible with the format. See [the docs](https://stac-utils.github.io/stac-geoparquet/latest/) for more information.

Because Parquet files are cloud-optimised and can be queried remotely, e.g. using DuckDB and its [httpfs extension](https://duckdb.org/docs/stable/core_extensions/httpfs/overview.html) or [rustac](https://stac-utils.github.io/rustac/), it is not necessary for a data consumer to download potentially large files prior to query.

STAC-GeoParquet does not provide a REST API interface and is instead accessed programmatically or via SQL. If a REST API is not required, and the primary goal is to query STAC data without the overhead of a data management platform, then STAC-GeoParquet may be a suitable solution.

#### Limitations

STAC-GeoParquet is not typically the primary storage format for STAC data. In some cases, such as some [Planetary Computer STAC collections](https://planetarycomputer.microsoft.com/api/stac/v1/collections/3dep-seamless), Parquet files are provided as collection-level assets so that API clients can access STAC item data without having to page through items via the STAC API. This approach requires data duplication and may experience some of the same limitations identified for this project. In cases where data duplication is required, the entire STAC item must be duplicated, not just certain indexed properties.

All STAC item properties, which would normally reside in a nested object within a STAC item object, must be "promoted" to top-level properties such that they can stored and queried as columns in a Parquet file. As a result property names are not allowed to duplicate the names of other top-level properties. Any properties that are not promoted do not exist in the resulting data.

STAC collections are stored as Parquet metadata, rather than as columnar data, and therefore are not searchable. Prior to introduction of the [collection-search STAC API extension](https://github.com/stac-api-extensions/collection-search) this would not be considered a limitation, as collection search was not previously standardised.

Since 1.1.0 STAC-GeoParquet does not _require_ each collection to exist in a different Parquet file, but this approach is encouraged. Where a STAC data provider has followed this convention it may be more difficult to effectively search across multiple collections.

#### stac-fastapi-geoparquet

The [stac-fastapi-geoparquet](https://pypi.org/project/stac-fastapi-geoparquet/) project aims to augment STAC-GeoParquet with a STAC API interface, however this project does not currently appear to offer a production-ready solution.
