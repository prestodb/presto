-- database: presto; groups: postgresql_connector; queryType: SELECT; tables: postgres.public.workers_psql
--!
describe postgresql.public.workers_psql
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | integer     | | |10|null|null|
first_name         | varchar(32) | | |null|null|32|
last_name          | varchar(32) | | |null|null|32|
date_of_employment | date        | | |null|null|null|
department         | integer     | | |10|null|null|
id_department      | integer     | | |10|null|null|
name               | varchar(32) | | |null|null|32|
salary             | integer     | | |10|null|null|
