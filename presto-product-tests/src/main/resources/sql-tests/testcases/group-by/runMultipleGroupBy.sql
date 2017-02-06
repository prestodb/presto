-- database: presto; tables: nation; groups: group-by;
SELECT COUNT(*), n_regionkey, n_nationkey FROM nation WHERE n_regionkey < 2 GROUP BY n_nationkey, n_regionkey ORDER BY n_regionkey, n_nationkey DESC
