# Index Configuration

The indexer expects an argument referencing a JSON index configuration file. This document describes that file's content.

## Required

The `root_catalog_uri` property is required and must reference a STAC catalog. Its path must include a prefix for which a compatible reader exists.

## Optional

Any number of queryable and sortable STAC properties may be configured.

### Indexables

The indexer requires knowledge of the DuckDB data type that can be used to store queryable or sortable properties. Because properties can be both queryable _and_ sortable this configuration is maintained in the `indexables` property to avoid duplication.

Entries in `queryables` and `sortables` must have a corresponding entry in `indexables`.

Each queryable and sortable property must include a list of collections for which the property is queryable or sortable. The `*` wildcard value can be used to indicate all collections.

### Queryables

Queryables require a `json_schema` property containing a schema that could be used to validate values of this property. This JSON schema is not used directly by the API but is provided to API clients via the `/queryables` endpoints such that a client can validate any value it intends to send as query value for this property.

The API requires special handling of geometry and temporal fields during SQL query construction. If a queryable property contains geometry it must include `is_geometry: true`. If a queryable property contains temporal data it must include `is_temporal: true`. These values default to `false`.

## Example 1

```json
{
    "root_catalog_uri": "s3://bucket/STAC/catalog.json"
}
```

## Example 2

```json
{
    "root_catalog_uri": "/data/catalog.json",
    "indexables": {
        "gsd": {
            "storage_type": "DOUBLE",
            "description": "Ground Sample Distance",
            "json_path": "properties.gsd"
        }
    },
    "queryables": {
        "gsd": {
            "json_schema":  {
                "type": "number",
                "exclusiveMinimum": 0
            },
            "collections": [
                "joplin"
            ]
        }
    },
    "sortables": {
        "gsd": {
            "collections": [
                "joplin"
            ]
        }
    }
}
```
