CREATE TABLE sortables (
    name VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL, -- '*' value indicates all collections
    description VARCHAR NOT NULL,
    items_column VARCHAR,
    PRIMARY KEY(name, collection_id),
    UNIQUE(items_column),
);