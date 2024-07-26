INSERT INTO queryables (name, description, json_path, json_schema, items_column, is_geometry, is_temporal) VALUES
    ('id', 'Item ID', 'id', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', NULL, false, false),
    ('collection', 'Collection ID', 'collection', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/collection"}', 'collection_id', false, false),
    ('geometry', 'Geometry', 'geometry', '{"$ref": "https://geojson.org/schema/Geometry.json"}', NULL, true, false),
    ('datetime', 'Datetime, NULL if datetime is a range', 'datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL, false, true),
    ('start_datetime', 'Start datetime if datetime is a range, NULL if not', 'start_datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL, false, true),
    ('end_datetime', 'End datetime if datetime is a range, NULL if not', 'end_datetime', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/datetime"}', NULL, false, true),
;
