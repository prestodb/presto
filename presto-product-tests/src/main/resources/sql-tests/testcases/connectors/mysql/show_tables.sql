-- database: presto; groups: mysql_connector; tables: mysql.workers_jdbc, mysql.datatype_jdbc
-- queryType: SELECT;
--!
show tables from mysql.test
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
datatype_jdbc|
workers_jdbc|
