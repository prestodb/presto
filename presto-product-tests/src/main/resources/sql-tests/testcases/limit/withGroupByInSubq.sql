-- database: presto; groups: limit; tables: partsupp
SELECT COUNT(*) FROM (
    SELECT ps_suppkey, COUNT(*) FROM partsupp
    GROUP BY ps_suppkey LIMIT 20) t1