-- database: presto; groups: mysql_connector; tables: mysql.workers_mysql, mysql.datatype_mysql, mysql.real_table_mysql
-- queryType: SELECT;
--!
show tables from mysql.test
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
datatype_mysql|
workers_mysql|
real_table_mysql|
