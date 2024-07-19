INSERT INTO sortables (name, collection_id, description, items_column) VALUES
    ('id', '*', 'Item ID', NULL),
    ('collection', '*', 'Collection ID', 'collection_id'),
    ('datetime', '*', 'Datetime, NULL if datetime is a range', NULL),
    ('start_datetime', '*', 'Start datetime if datetime is a range, NULL if not', NULL),
    ('end_datetime', '*', 'End datetime if datetime is a range, NULL if not', NULL),
;
