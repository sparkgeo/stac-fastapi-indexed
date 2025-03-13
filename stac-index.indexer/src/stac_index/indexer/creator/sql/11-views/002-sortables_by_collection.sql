CREATE VIEW sortables_by_collection AS
    SELECT s.name
         , sc.collection_id
         , s.description
         , s.json_path
         , COALESCE(s.items_column, s.name) AS items_column
      FROM sortables s
      JOIN sortables_collections sc ON s.name == sc.name
     UNION
    SELECT s.name
         , '*' AS collection_id
         , s.description
         , s.json_path
         , COALESCE(s.items_column, s.name) AS items_column
      FROM sortables s 
      JOIN (
           SELECT sac.name
             FROM sortables_all_collections sac
      ) sac ON sac.name = s.name
;