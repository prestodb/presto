-- database: presto; groups: system
SELECT
  catalog_name,
  schema_name
FROM system.information_schema.schemata
