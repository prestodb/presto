-- database: presto; groups: postgresql_connector; tables: postgres.datatype_psql,postgres.workers_jdbc,postgres.real_table_psql
-- queryType: SELECT;
--!
show tables from postgresql.public
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
datatype_psql|
workers_jdbc|
real_table_psql|
