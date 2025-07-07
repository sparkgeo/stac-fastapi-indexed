INSERT INTO sortables (name, description, json_path, items_column, json_type) VALUES
    ('id', 'Item ID', 'id', NULL, 'string'),
    ('collection', 'Collection ID', 'collection', 'collection_id', 'string'),
    ('datetime', 'Datetime, NULL if datetime is a range', 'datetime', NULL, 'string'),
    ('start_datetime', 'Start datetime if datetime is a range, NULL if not', 'start_datetime', NULL, 'string'),
    ('end_datetime', 'End datetime if datetime is a range, NULL if not', 'end_datetime', NULL, 'string'),
;
