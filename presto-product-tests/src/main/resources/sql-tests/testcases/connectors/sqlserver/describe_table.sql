-- database: presto; groups: sqlserver, profile_specific_tests; queryType: SELECT; tables: sqlserver.dbo.workers_sqlserver
--!
describe sqlserver.dbo.workers_sqlserver
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | | |10|null|null|
first_name         | varchar(32) | | |null|null|32|
last_name          | varchar(32) | | |null|null|32|
date_of_employment | date        | | |null|null|null|
department         | tinyint     | | |3|null|null|
id_department      | integer     | | |10|null|null|
name               | varchar(32) | | |null|null|32|
salary             | integer     | | |10|null|null|
