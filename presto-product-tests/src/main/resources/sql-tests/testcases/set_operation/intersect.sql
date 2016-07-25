-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: intersect_and_union
SELECT n_name FROM nation WHERE n_nationkey = 17 
INTERSECT 
SELECT n_name FROM nation WHERE n_regionkey = 1 
UNION 
SELECT n_name FROM nation WHERE n_regionkey = 2
--!
INDIA|
INDONESIA|
CHINA|
JAPAN|
VIETNAM|
PERU|
--! name: intersect_null
SELECT id_employee
FROM workers
INTERSECT
SELECT department
FROM workers
--!
null|
9|
4|
7|
2|
5|
8|
