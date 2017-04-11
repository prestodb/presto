-- database: presto; tables: nation; groups: group-by;
SELECT COUNT(*) FROM nation HAVING COUNT(*) > 20
