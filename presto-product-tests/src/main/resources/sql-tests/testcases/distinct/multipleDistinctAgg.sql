-- database: presto; groups: distinct; tables: nation
SELECT COUNT(DISTINCT n_regionkey), COUNT(DISTINCT n_name), MIN(DISTINCT n_nationkey) FROM nation
