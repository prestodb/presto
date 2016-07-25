-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: except_union_intersect
SELECT n_name FROM nation WHERE n_nationkey = 17
EXCEPT
SELECT n_name FROM nation WHERE n_regionkey = 2
UNION
(SELECT n_name FROM nation WHERE n_regionkey = 2
INTERSECT
SELECT n_name FROM nation WHERE n_nationkey > 15)
--!
CHINA|
VIETNAM|
PERU|
--! name: except_union_all_intersect
SELECT n_name FROM nation WHERE n_nationkey = 17
EXCEPT
SELECT n_name FROM nation WHERE n_regionkey = 2
UNION ALL
(SELECT n_name FROM nation WHERE n_regionkey = 2
INTERSECT
SELECT n_name FROM nation WHERE n_nationkey > 15)
--!
CHINA|
VIETNAM|
PERU|
--! name: except_null
SELECT id_employee FROM workers
EXCEPT
SELECT department FROM workers where department IS NOT NULL
--!
null|
3|
6|
10|
1|
