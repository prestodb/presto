-- database: presto; groups: distinct; tables: nation
SELECT n_regionkey, COUNT(DISTINCT n_name) FROM nation
GROUP BY n_regionkey
HAVING n_regionkey < 4
