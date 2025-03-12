CREATE SEQUENCE seq_errors_id START 1;

CREATE TABLE errors (
    id             INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_errors_id'),
    time           TIMESTAMPTZ,
    error_type     VARCHAR,
    subtype        VARCHAR,
    input_location VARCHAR,
    description    VARCHAR,
    possible_fixes VARCHAR
);
