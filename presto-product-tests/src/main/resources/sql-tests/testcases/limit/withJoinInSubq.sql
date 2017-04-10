-- database: presto; groups: limit; tables: nation
SELECT COUNT(*) FROM (SELECT * FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey LIMIT 5) foo