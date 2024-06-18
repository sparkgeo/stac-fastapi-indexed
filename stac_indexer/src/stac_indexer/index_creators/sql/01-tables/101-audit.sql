CREATE TABLE audit (
    event VARCHAR,
    time TIMESTAMPTZ,
    notes VARCHAR,
    PRIMARY KEY(event, time),
);
