-- database: presto; groups: mysql_connector; queryType: SELECT; tables: mysql.test.workers_mysql
--!
describe mysql.test.workers_mysql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | | |
first_name         | varchar(32) | | |
last_name          | varchar(32) | | |
date_of_employment | date        | | |
department         | tinyint     | | |
id_department      | integer     | | |
name               | varchar(32) | | |
salary             | integer     | | |
