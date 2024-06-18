CREATE TABLE queryables (
    collection_id VARCHAR NOT NULL, -- '*' value indicates all collections
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    json_path VARCHAR NOT NULL,
    json_schema VARCHAR NOT NULL,
    UNIQUE (collection_id, name),
);