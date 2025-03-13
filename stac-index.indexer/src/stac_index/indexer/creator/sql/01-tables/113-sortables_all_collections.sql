CREATE TABLE sortables_all_collections (
    name VARCHAR PRIMARY KEY,
    FOREIGN KEY (name) REFERENCES sortables (name),
);
