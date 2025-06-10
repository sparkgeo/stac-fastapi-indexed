CREATE TABLE collections (
    id VARCHAR PRIMARY KEY,
    stac_location VARCHAR NOT NULL,
    load_id VARCHAR(32) NOT NULL,
    collection_hash VARCHAR NOT NULL,
);
