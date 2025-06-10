CREATE TABLE index_history (
    id VARCHAR(32) PRIMARY KEY
  , start_time TIMESTAMPTZ NOT NULL
  , end_time TIMESTAMPTZ NOT NULL
  , root_catalog_uris VARCHAR[] NOT NULL
  , items_loaded BIGINT NOT NULL
  , items_added BIGINT NOT NULL
  , items_removed BIGINT NOT NULL
  , items_updated BIGINT NOT NULL
  , items_unchanged BIGINT NOT NULL
  , collections_loaded BIGINT NOT NULL
  , collections_added BIGINT NOT NULL
  , collections_removed BIGINT NOT NULL
  , collections_updated BIGINT NOT NULL
  , collections_unchanged BIGINT NOT NULL
);
