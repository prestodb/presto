-- database: presto; groups: system
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type
FROM system.information_schema.tables
