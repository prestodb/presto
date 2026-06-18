-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH wnation AS (SELECT n_name, n_nationkey, n_regionkey FROM nation)
SELECT n1.n_name, n2.n_name FROM wnation n1 JOIN wnation n2
ON n1.n_nationkey=n2.n_regionkey
