-- database: presto; tables: nation; groups: union;
SELECT count(*)
FROM nation
UNION ALL
SELECT sum(n_nationkey)
FROM nation
GROUP BY n_regionkey
