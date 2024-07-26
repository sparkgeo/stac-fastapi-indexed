INSERT INTO sortables (name, description, json_path, items_column) VALUES
    ('id', 'Item ID', 'id', NULL),
    ('collection', 'Collection ID', 'collection', 'collection_id'),
    ('datetime', 'Datetime, NULL if datetime is a range', 'datetime', NULL),
    ('start_datetime', 'Start datetime if datetime is a range, NULL if not', 'start_datetime', NULL),
    ('end_datetime', 'End datetime if datetime is a range, NULL if not', 'end_datetime', NULL),
;
