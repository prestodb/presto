-- database: presto; groups: limit; tables: nation
SELECT COUNT(*), n_regionkey FROM nation GROUP BY n_regionkey
ORDER BY n_regionkey DESC
LIMIT 2