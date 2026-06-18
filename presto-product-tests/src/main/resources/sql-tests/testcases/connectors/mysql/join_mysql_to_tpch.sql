-- database: presto; groups: mysql_connector; tables: mysql.test.workers_mysql
--!
select t1.first_name, t2.name from mysql.test.workers_mysql t1, tpch.sf1.nation t2 where t1.id_department = t2.nationkey
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
 null        | ARGENTINA|
 Ann         | BRAZIL|
 Martin      | BRAZIL|
 null        | CANADA|
 Joana       | EGYPT|
 Kate        | ETHIOPIA|
 Christopher | ETHIOPIA|
 null        | FRANCE|
 George      | GERMANY|
 Jacob       | INDIA|
 John        | INDONESIA|
 null        | IRAN|
