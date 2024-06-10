CREATE TABLE items (
    id VARCHAR PRIMARY KEY,
    collection_id VARCHAR NOT NULL REFERENCES collections(id),  /* STAC spec says collection can be null, but implementations like pgstac do not permit this. We should do whatever makes most sense for our use-case */
    geometry GEOMETRY NOT NULL,
    bbox_llx FLOAT NOT NULL,
    bbox_lly FLOAT NOT NULL,
    bbox_urx FLOAT NOT NULL,
    bbox_ury FLOAT NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    datetime_end TIMESTAMPTZ,
    cloud_cover FLOAT,
    stac_location VARCHAR NOT NULL,
);