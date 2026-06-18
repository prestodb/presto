-- database: presto; groups: sqlserver, profile_specific_tests; tables: sqlserver.dbo.workers_sqlserver
--!
select t1.last_name, t2.first_name from sqlserver.dbo.workers_sqlserver t1, sqlserver.dbo.workers_sqlserver t2 where t1.id_department = t2.id_employee
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
 null      | Mary|
 Turner    | Ann|
 Smith     | Ann|
 null      | Martin|
 Donne     | Joana|
 Grant     | Kate|
 Johnson   | Kate|
 null      | Christopher|
 Cage      | George|
 Brown     | Jacob|
 Black     | John|
 null      | Charlie|
