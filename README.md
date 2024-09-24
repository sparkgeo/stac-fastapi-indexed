# STAC API Serverless

[![Pull Request, Any Branch](https://github.com/sparkgeo/STAC-API-Serverless/actions/workflows/pull-request-all.yml/badge.svg)](https://github.com/sparkgeo/STAC-API-Serverless/actions/workflows/pull-request-all.yml)

A [stac-fastapi](https://github.com/stac-utils/stac-fastapi) backend that indexes a catalog to Parquet to make it searchable. The ability to work with static files, and the lack of a need for another persistent data store such as a database, mean this backend can run in a serverless environment.

This backend does not support transactions.

## Known Issues

See [Known Issues](./docs/known-issues.md) for more information.

## Development

See [Development](./docs/development.md) for guidance on how to work on this project.

## Quickstart

To get a quick demo up and running execute any of the following scripts and navigate to http://localhost:8123/api.html

```sh
scripts/run-with-local-s3.sh    # loads a sample dataset into minio, indexes it, loads the index into minio, and runs the API
scripts/run-with-local-file.sh  # indexes a sample dataset on the filesystem and runs the API
scripts/run-with-local-http.sh  # loads a sample dataset into a HTTP fileserver, indexes it, and runs the API
scripts/run-with-remote-http.sh https://capella-open-data.s3.us-west-2.amazonaws.com/stac/catalog.json # indexes a public static STAC catalog over HTTPS and runs the API
```

## Overview

This repository supports two related but distinct behaviours. The `stac-index.*` packages defined in [./stac_index/](./stac_index/) manage indexing of a STAC catalog. The `stac-fastapi.indexed` package defined in [./stac_fastapi/](./stac_fastapi/) implements a stac-fastapi backend that works with the index created by `stac-index.*` packages.

The indexer might only be run once on a stable dataset, or may be run repeatedly for a changing dataset (see [Limitations / Incremental Updates](#incremental-updates) for multiple-run concerns). The stac-fastapi backend will consult the index once for every request the API receives.

There is no requirement for the API to run in a serverless environment - in some cases the associated performance concerns may be unacceptable - though its ability to run in a serverless environment is a key factor in its design.

![Diagram showing an overview of the relationship between the indexer and API](./docs/diagrams/exports/Overview.png "Overview Diagram")

### Readers

Source and Index Readers support extensibility in the design and determine where and how this design can be used.

STAC objects (catalog, collections, items) are identified by a unique URI. Each URI's format determines which reader must be used to read its content.
- The [S3 Reader](./stac_index/reader/s3/) is used to read any content identified by a URI with a `s3://` prefix.
- The [Filesystem Reader](./stac_index/reader/filesystem/) is used to read any content identified by a URI with a `/` prefix.
- The [HTTPS Reader](./stac_index/reader/https/) is used to read any content identified by a URI with a `http(s)://` prefix.

An error will be thrown if an unsupported URI prefix is encountered.

### Indexer 

The indexer is configured with a root catalog URI. From the root catalog the indexer traverses the catalog's sub-catalogs (if present), collections and items, relying on STAC's `links` objects to provide their URIs.

The indexer selects a suitable reader for each URI as described in [Readers](#readers). In this way the indexer can index a local STAC catalog, a remote S3-hosted STAC catalog (assuming any auth concerns are addressed), a HTTP(S)-hosted STAC catalog, and potentially any number of other STAC storage methods.

The indexer creates an index entry for every STAC collection and item in the catalog. By default each item's index entry includes the item's ID and collection, which together form its unique identifier, and values of the minimum set of properties required to support the API's base requirements. This minimum set of properties includes datetimes, BBOX, and the URI of the item's full content.

Only necessary properties from the STAC catalog are indexed, rather than copying the entire catalog to Parquet, to maximise query performance and minimise data duplication.

The indexer can be configured with any number of queryable and / or sortable fields. Any fields identified as queryable or sortable will also be indexed by the indexer. See [Index Configuration](./docs/index-config.md) for more information on indexer configuration.

The indexer produces a number of Parquet files, which must be made accessible in some way to the API.

![Diagram showing the process of indexing a STAC catalog](./docs/diagrams/exports/Indexer%20Process.png "Indexer Process")

### API

The API is confiugured with an index source URI which identifies the location of Parquet index files. Index files are enumerated using that URI via a reader as described in [Readers](#readers). During API operation Parquet files are read by DuckDB and therefore can only be accessed via a method supported by DuckDB. While there is flexibility in how STAC content can be accessed by both the indexer and the API, Parquet index files should only be accessed via either the S3 or Filesystem readers. The API accesses Parquet index files in read-only mode and cannot modify them.

For each reqeust the API constructs an SQL query to run against the STAC catalog's index. DuckDB is responsible for satisfying SQL queries and manages interaction with Parquet data. The SQL query returns zero or more URIs referencing collections or items that satisfy the request. The API retrieves the full STAC content from those URIs, using the appropriate reader, and returns a response.

![Diagram showing the process of handling an API request](./docs/diagrams/exports/Query%20Process.png "API Request Process")

## Limitations

This project is currently subject to a number of limitations. These limitations are not believed to be insurmountable; they have simply not yet received sufficient attention.

### Incremental Updates

Development and testing has so far assumed a one-time indexing process and no need to support rolling incremental updates to the STAC catalog and its index.

For STAC catalogs with infrequent and atomic updates, and a reasonably-sized catalog, it may be sufficient to re-index the catalog following each update and entirely replace the Parquet index files as required.

New development work will be required to support STAC catalogs with frequent, incremental, or unpredictable updates. In principle there is no reason that the indexer could not support change detection and the ability to add, update, or remove only affected data. This process would likely be complicated by the addition or removal of queryable or sortable fields after the initial indexer run.

There is an open question regarding how the API should handle data updates during normal operation. There is no limit on how much time can pass between a client receiving a multi-page response with a `next` or `previous` paging link and when it accesses those links. It is possible for the set matching the initial request to change during a client's paging. The size of the set or its order could change between pages, creating inconsistency. If a request includes queryable or sortable properties it is possible that one or more fields are no longer queryable or sortable following an update, likely resulting in an error. The STAC API specification does not define expected behaviour in data update scenarios and [this question](https://github.com/radiantearth/stac-api-spec/issues/453) on the subject is unanswered at the time of writing.
