-- database: presto; groups: mysql_connector; queryType: SELECT; tables: mysql.test.workers_mysql
--!
describe mysql.test.workers_mysql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | | |10 |null |null |
first_name         | varchar(32) | | |null |null |32 |
last_name          | varchar(32) | | |null |null | 32|
date_of_employment | date        | | |null |null |null |
department         | tinyint     | | | 3 |null | null|
id_department      | integer     | | | 10|null |null |
name               | varchar(32) | | | null| null| 32|
salary             | integer     | | |10 |null | null|
