CREATE VIEW queryables_by_collection AS
    SELECT q.collection_id, q.name, q.description, q.json_path, q.json_schema
      FROM queryables q
      JOIN collections c ON c.id = q.collection_id
     UNION
    SELECT c.id AS collection_id, q.name, q.description, q.json_path, q.json_schema
      FROM collections c 
CROSS JOIN queryables q
     WHERE q.collection_id = '*';
