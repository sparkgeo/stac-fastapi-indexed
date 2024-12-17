CREATE SEQUENCE seq_collections_id START 1;

CREATE TABLE errors (
    id             INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_collections_id'),
    time           TIMESTAMPTZ,
    error_type     VARCHAR,
    subtype        VARCHAR,
    input_location VARCHAR,
    description    VARCHAR
);
