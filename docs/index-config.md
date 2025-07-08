# Index Configuration

The indexer requires exactly one of the following arguments
- `--root_catalog_uri` referencing the location of a STAC catalog JSON.
- `--manifest_json_uri` referencing the index manifest from a prior indexer run.

When indexing a new STAC catalog (i.e. not updating an existing index) the indexer can optionally accept an argument referencing a JSON index configuration file, which offers greater control over indexer behaviour. The following describes that file's content.

## Optional Properties

### Indexables

The indexer requires knowledge of the DuckDB data type that can be used to store queryable or sortable properties. Because properties can be both queryable _and_ sortable this configuration is maintained in the `indexables` property to avoid duplication.

Entries in `queryables` and `sortables` must have a corresponding entry in `indexables`.

Each queryable and sortable property must include a list of collections for which the property is queryable or sortable. The `*` wildcard value can be used to indicate all collections. It is **not** currently possible to wildcard partial collection IDs, such as `collection-*`.

`storage_type` **must** reference a valid [DuckDB data type](https://duckdb.org/docs/stable/sql/data_types/overview.html).

### Queryables

Queryables require a `json_schema` property containing a schema that could be used to validate values of this property. This JSON schema is not used directly by the API but is provided to API clients via the `/queryables` endpoints such that a client can validate any value it intends to send as query value for this property.

### Fixes

The indexer attempts to parse STAC item JSON using [stac-pydantic](https://pypi.org/project/stac-pydantic/). stac-pydantic is not particularly lenient and will reject invalid JSON, resulting in the STAC item not being indexed and an error in the indexer log. This may be valid in some use-cases, but in cases where STAC item JSON cannot be fixed, and may not be owned or controlled by the indexer's user, it might be preferable to index invalid JSON. The indexer supports a `fixes_to_apply` property. This property accepts a list of fixer names to attempt to apply to invalid JSON. Fixers are defined [in code](../packages/stac-index/src/stac_index/indexer/stac_parser.py) and must exist before being referenced here. The list of available fixers is currently short and may be expanded in future to accommodate common validity problems.

## Example

```json
{
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
    },
    "fixes_to_apply": [
        "eo-extension-uri"
    ]
}
```
