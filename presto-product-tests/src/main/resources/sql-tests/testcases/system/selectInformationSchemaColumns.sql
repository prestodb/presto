-- database: presto; groups: system
select
  table_catalog,
  table_schema,
  table_name,
  column_name,
  ordinal_position,
  column_default,
  is_nullable,
  data_type,
  comment
from SYSTEM.information_schema.columns
