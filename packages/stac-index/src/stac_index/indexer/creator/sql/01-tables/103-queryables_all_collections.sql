CREATE TABLE queryables_all_collections (
    name VARCHAR PRIMARY KEY,
    FOREIGN KEY (name) REFERENCES queryables (name),
);
