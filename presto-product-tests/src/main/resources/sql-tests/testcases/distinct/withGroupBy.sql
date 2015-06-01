-- database: presto; groups: distinct; tables: nation
SELECT DISTINCT n_regionkey, COUNT(*) FROM nation
WHERE n_nationkey > 0
GROUP BY n_regionkey
