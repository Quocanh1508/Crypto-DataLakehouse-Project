-- Register existing Delta tables written by Spark under GCS (bronze + silver).
-- Requires: delta.register-table-procedure.enabled=true in trino/catalog/delta.properties
-- Run: docker exec -it trino trino --catalog delta --execute "$(cat sql/register_delta_tables.sql)"
-- Or paste into Trino CLI / JDBC against http://localhost:8080

CALL delta.system.register_table(
  schema_name => 'default',
  table_name => 'crypto_trades_bronze',
  table_location => 'gs://crypto-lakehouse-group8/bronze'
);

CALL delta.system.register_table(
  schema_name => 'default',
  table_name => 'crypto_trades_silver',
  table_location => 'gs://crypto-lakehouse-group8/silver'
);

-- Smoke checks:
-- SHOW TABLES FROM delta.default;
-- SELECT count(*) FROM delta.default.crypto_trades_bronze LIMIT 1;
-- SELECT count(*) FROM delta.default.crypto_trades_silver LIMIT 1;
