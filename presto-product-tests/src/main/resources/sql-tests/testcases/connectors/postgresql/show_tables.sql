-- database: presto; groups: postgresql_connector; tables: postgres.public.datatype_psql,postgres.public.workers_psql,postgres.public.real_table_psql
-- queryType: SELECT;
--!
show tables from postgresql.public
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
datatype_psql|
workers_psql|
real_table_psql|
