CREATE VIEW queryables_by_collection AS
    SELECT q.name
         , qc.collection_id
         , q.description
         , q.json_path
         , q.json_schema
         , COALESCE(q.items_column, q.name) AS items_column
         , q.is_geometry
         , q.is_temporal
      FROM queryables q
      JOIN queryables_collections qc ON q.name == qc.name
     UNION
    SELECT q.name
         , '*' AS collection_id
         , q.description
         , q.json_path
         , q.json_schema
         , COALESCE(q.items_column, q.name) AS items_column
         , q.is_geometry
         , q.is_temporal
      FROM queryables q 
      JOIN (
           SELECT qac.name
             FROM queryables_all_collections qac
      ) qac ON qac.name = q.name
;