-- database: presto; tables: nation; groups: group-by;
SELECT COUNT(n_regionkey) FROM nation WHERE 1=2 HAVING SUM(n_regionkey) IS NULL
