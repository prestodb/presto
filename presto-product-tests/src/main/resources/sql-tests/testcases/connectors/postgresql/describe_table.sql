-- database: presto; groups: postgresql_connector; queryType: SELECT; tables: postgres.workers_jdbc
--!
describe postgresql.public.workers_jdbc
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
id_employee        | bigint  | true | false | |
first_name         | varchar | true | false | |
last_name          | varchar | true | false | |
date_of_employment | date    | true | false | |
department         | bigint  | true | false | |
id_department      | bigint  | true | false | |
name               | varchar | true | false | |
salary             | bigint  | true | false | |
