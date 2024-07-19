CREATE TABLE sortables (
    name VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    json_path VARCHAR NOT NULL,
    items_column VARCHAR,
    UNIQUE(json_path),
    UNIQUE(items_column),
);