-- database: presto; groups: mysql_connector; tables: mysql.test.workers_mysql
--!
select t1.last_name, t2.first_name from mysql.test.workers_mysql t1, mysql.test.workers_mysql t2 where t1.id_department = t2.id_employee
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
