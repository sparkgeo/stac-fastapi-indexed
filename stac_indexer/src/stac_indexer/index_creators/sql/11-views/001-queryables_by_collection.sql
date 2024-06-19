CREATE VIEW queryables_by_collection AS
    SELECT q.name
         , q.collection_id
         , q.description
         , q.json_path
         , q.json_schema
         , COALESCE(q.items_column, q.name) as items_column
         , is_geometry
      FROM queryables q
      JOIN collections c ON c.id = q.collection_id
     UNION
    SELECT q.name
         , c.id AS collection_id
         , q.description
         , q.json_path
         , q.json_schema
         , COALESCE(q.items_column, q.name) as items_column
         , is_geometry
      FROM collections c 
CROSS JOIN queryables q
     WHERE q.collection_id = '*';
