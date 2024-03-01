-- database: presto; tables: nation; groups: union;
SELECT *
FROM nation
UNION DISTINCT
SELECT *
FROM nation
