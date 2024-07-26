CREATE TABLE queryables (
    name VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    json_path VARCHAR NOT NULL,
    json_schema VARCHAR NOT NULL,
    items_column VARCHAR,
    is_geometry BOOLEAN DEFAULT false,
    is_temporal BOOLEAN DEFAULT false,
    UNIQUE(json_path),
    UNIQUE(items_column),
);