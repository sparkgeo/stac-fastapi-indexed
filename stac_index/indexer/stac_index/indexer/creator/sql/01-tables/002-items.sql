CREATE TABLE items (
    id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL REFERENCES collections(id),  /* STAC spec says collection can be null, but implementations like pgstac do not permit this. We should do whatever makes most sense for our use-case */
    geometry GEOMETRY NOT NULL,
    datetime TIMESTAMPTZ,
    start_datetime TIMESTAMPTZ,
    end_datetime TIMESTAMPTZ,
    stac_location VARCHAR NOT NULL,
    PRIMARY KEY (collection_id, id),
);