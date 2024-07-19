CREATE VIEW sortables_by_collection AS
    SELECT s.name
         , s.collection_id
         , s.description
         , COALESCE(s.items_column, s.name) as items_column
      FROM sortables s
      JOIN collections c ON c.id = s.collection_id
     UNION
    SELECT s.name
         , s.collection_id
         , s.description
         , COALESCE(s.items_column, s.name) as items_column
      FROM collections c
CROSS JOIN sortables s
     WHERE s.collection_id = '*';
