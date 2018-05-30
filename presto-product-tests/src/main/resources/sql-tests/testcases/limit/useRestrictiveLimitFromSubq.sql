-- database: presto; groups: limit; tables: nation
SELECT COUNT(*) FROM (SELECT * FROM nation LIMIT 2) AS foo LIMIT 5