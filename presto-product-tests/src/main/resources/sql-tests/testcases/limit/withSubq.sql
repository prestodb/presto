-- database: presto; groups: limit; tables: nation
SELECT COUNT(*) FROM (SELECT * FROM nation LIMIT 10) t1