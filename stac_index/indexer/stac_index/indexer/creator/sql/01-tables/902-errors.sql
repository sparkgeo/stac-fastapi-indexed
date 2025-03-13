CREATE SEQUENCE seq_errors_id START 1;

CREATE TABLE errors (
    id             INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_errors_id'),
    time           TIMESTAMPTZ,
    error_type     VARCHAR(50) NOT NULL,
    subtype        VARCHAR NULL,
    input_location VARCHAR NULL,
    description    VARCHAR NOT NULL,
    possible_fixes VARCHAR NULL,
    collection     VARCHAR NULL,
    item           VARCHAR NULL,
);
