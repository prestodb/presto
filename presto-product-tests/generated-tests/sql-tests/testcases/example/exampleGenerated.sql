-- database: presto; groups: generated_tests
SELECT
  table_catalog,
  table_schema,
  table_name,
  column_name,
  ordinal_position,
  column_default,
  is_nullable,
  data_type,
  is_partition_key,
  comment
FROM SYSTEM.information_schema.columns