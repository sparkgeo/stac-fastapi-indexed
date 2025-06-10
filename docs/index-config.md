# Index Configuration

The indexer requires exactly one of the following arguments
- `--root_catalog_uri` referencing the location of a STAC catalog JSON
- `--manifest_json_uri` referencing the index manifest from a prior indexer run

The indexer can optionally accept an argument referencing a JSON index configuration file, which offers greater control over indexer behaviour. The following describes that file's content.

## Optional Properties

Any number of queryable and sortable STAC properties may be configured.

### Indexables

The indexer requires knowledge of the DuckDB data type that can be used to store queryable or sortable properties. Because properties can be both queryable _and_ sortable this configuration is maintained in the `indexables` property to avoid duplication.

Entries in `queryables` and `sortables` must have a corresponding entry in `indexables`.

Each queryable and sortable property must include a list of collections for which the property is queryable or sortable. The `*` wildcard value can be used to indicate all collections.

### Queryables

Queryables require a `json_schema` property containing a schema that could be used to validate values of this property. This JSON schema is not used directly by the API but is provided to API clients via the `/queryables` endpoints such that a client can validate any value it intends to send as query value for this property.

## Example

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
