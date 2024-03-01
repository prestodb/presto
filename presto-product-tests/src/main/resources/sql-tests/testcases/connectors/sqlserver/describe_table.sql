-- database: presto; groups: sqlserver, profile_specific_tests; queryType: SELECT; tables: sqlserver.dbo.workers_sqlserver
--!
describe sqlserver.dbo.workers_sqlserver
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
