-- db/bootstrap/tsdb-roles.sql
-- 运行：psql -U postgres -d thermio_ts \
--        -v ingest_password="$THERMIO_TSDB_INGEST_PASSWORD" \
--        -v api_password="$THERMIO_TSDB_API_PASSWORD" \
--        -v algo_password="$THERMIO_TSDB_ALGO_PASSWORD" -f db/bootstrap/tsdb-roles.sql
\set ON_ERROR_STOP on
CREATE ROLE tsdb_ingest LOGIN PASSWORD :'ingest_password';
CREATE ROLE tsdb_api    LOGIN PASSWORD :'api_password';
CREATE ROLE tsdb_algo   LOGIN PASSWORD :'algo_password';
GRANT CONNECT ON DATABASE thermio_ts TO tsdb_ingest, tsdb_api, tsdb_algo;
