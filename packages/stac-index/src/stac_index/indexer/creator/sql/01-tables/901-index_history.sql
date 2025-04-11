CREATE TABLE index_history (
    id VARCHAR(32) PRIMARY KEY
  , start_time TIMESTAMPTZ NOT NULL
  , end_time TIMESTAMPTZ NULL
  , root_catalog_uris VARCHAR[]
  , loaded BIGINT
  , added BIGINT
  , removed BIGINT
  , updated BIGINT
  , unchanged BIGINT
);
