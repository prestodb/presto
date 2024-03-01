-- database: presto; groups: distinct; tables: nation
SELECT COUNT(DISTINCT n_regionkey), COUNT(*) FROM nation
