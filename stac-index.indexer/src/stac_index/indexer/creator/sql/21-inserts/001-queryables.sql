INSERT INTO queryables (name, description, json_path, json_schema, items_column) VALUES
    ('id', 'Item ID', 'id', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', NULL),
    ('collection', 'Collection ID', 'collection', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/collection"}', 'collection_id'),
    ('geometry', 'Geometry', 'geometry', '{"$ref": "https://geojson.org/schema/Geometry.json"}', NULL),
    ('datetime', 'Datetime, NULL if datetime is a range', 'datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL),
    ('start_datetime', 'Start datetime if datetime is a range, NULL if not', 'start_datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL),
    ('end_datetime', 'End datetime if datetime is a range, NULL if not', 'end_datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL),
;
