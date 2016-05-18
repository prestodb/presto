-- database: presto; groups: mysql_connector; queryType: SELECT; tables: mysql.workers_jdbc
--!
describe mysql.test.workers_jdbc
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | |
first_name         | varchar(32) | |
last_name          | varchar(32) | |
date_of_employment | date        | |
department         | integer     | |
id_department      | integer     | |
name               | varchar(32) | |
salary             | integer     | |
