CREATE TABLE queryables (
    name VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL, -- '*' value indicates all collections
    description VARCHAR NOT NULL,
    json_path VARCHAR NOT NULL,
    json_schema VARCHAR NOT NULL,
    items_column VARCHAR,
    is_geometry BOOLEAN DEFAULT false,
    is_temporal BOOLEAN DEFAULT false,
    PRIMARY KEY(name, collection_id),
    UNIQUE(json_path),
    UNIQUE(items_column),
);