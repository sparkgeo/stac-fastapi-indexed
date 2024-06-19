INSERT INTO queryables (name, collection_id, description, json_path, json_schema, items_column, is_geometry) VALUES
    ('id', '*', 'Item ID', 'id', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', NULL, false),
    ('collection', '*', 'Collection ID', 'collection', '{"$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/collection"}', 'collection_id', false),
    ('geometry', '*', 'Geometry', 'geometry', '{"$ref": "https://geojson.org/schema/Geometry.json"}', NULL, true),
;
