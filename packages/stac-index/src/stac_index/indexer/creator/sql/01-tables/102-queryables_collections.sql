CREATE TABLE queryables_collections (
    name VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    PRIMARY KEY(name, collection_id),
    FOREIGN KEY (name) REFERENCES queryables (name),
    FOREIGN KEY (collection_id) REFERENCES collections (id),
);
