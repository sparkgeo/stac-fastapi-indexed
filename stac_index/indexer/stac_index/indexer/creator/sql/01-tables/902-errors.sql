CREATE SEQUENCE seq_collections_id START 1;

CREATE TABLE errors (
    id          INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_collections_id'),
    error_type  VARCHAR,
    time        TIMESTAMPTZ,
    description VARCHAR
);
